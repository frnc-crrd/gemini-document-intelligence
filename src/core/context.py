"""Gestor de contexto universal (Memoria del Sistema).

Adaptado para la arquitectura Map-Reduce. Persiste la memoria global 
del sistema basándose en la metadata tabular generada tras la 
Fase Reduce, garantizando la trazabilidad sin acoplarse a modelos estrictos.
"""

import json
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

try:
    from botocore.exceptions import ClientError
except ImportError:
    ClientError = Exception

from src.config import get_settings
from src.core.logger import get_system_logger

logger = get_system_logger(__name__)
settings = get_settings()


class StorageStrategy(ABC):
    """Interfaz abstracta para las estrategias de persistencia de contexto."""

    @abstractmethod
    def load(self) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def save(self, data: Dict[str, Any]) -> None:
        pass


class LocalStorageStrategy(StorageStrategy):
    """Estrategia de persistencia en el sistema de archivos local."""

    def __init__(self, file_path: Path):
        self.file_path = file_path

    def load(self) -> Optional[Dict[str, Any]]:
        try:
            if not self.file_path.exists():
                return None
            with open(self.file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Corrupción detectada en el archivo de contexto local: {e}", exc_info=True)
            return None

    def save(self, data: Dict[str, Any]) -> None:
        try:
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except PermissionError as e:
            logger.error(f"Error de permisos al guardar el contexto local: {e}", exc_info=True)
            raise


class S3StorageStrategy(StorageStrategy):
    """Estrategia de persistencia en AWS S3 utilizando inyección de dependencias."""

    def __init__(self, s3_client: Any, bucket_name: str, object_key: str):
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.object_key = object_key

    def load(self) -> Optional[Dict[str, Any]]:
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.object_key)
            content = response['Body'].read().decode('utf-8')
            return json.loads(content)
        except self.s3_client.exceptions.NoSuchKey:
            return None
        except ClientError as e:
            logger.error(f"Error de red/permisos al acceder a S3: {e.response['Error']['Message']}", exc_info=True)
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Corrupción detectada en el payload S3: {e}", exc_info=True)
            return None

    def save(self, data: Dict[str, Any]) -> None:
        try:
            payload = json.dumps(data, indent=2, ensure_ascii=False)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.object_key,
                Body=payload.encode('utf-8')
            )
        except ClientError as e:
            logger.error(f"Fallo al escribir en S3: {e.response['Error']['Message']}", exc_info=True)
            raise


class SystemContextManager:
    """Gestiona la memoria colectiva del sistema aislando la lógica de negocio del almacenamiento."""

    def __init__(self, storage_strategy: StorageStrategy):
        self.storage = storage_strategy

    def obtener_contexto_actual(self) -> Dict[str, Any]:
        """Recupera el estado global actual o inicializa uno nuevo si hay un fallo."""
        context = self.storage.load()
        if context is None:
            logger.info("Retornando estructura base del sistema.")
            return self._estructura_base()
        return context

    def generar_prompt_contexto(self) -> str:
        """Construye la representación textual de la memoria para el agente de IA."""
        contexto = self.obtener_contexto_actual()
        
        lineas = [
            "=== CONTEXTO HISTÓRICO DEL SISTEMA ===",
            "Utiliza esta información para resolver ambigüedades."
        ]
        
        patrones = contexto.get("patrones_folio", {})
        if patrones:
            lineas.append("\nPATRONES DE FOLIO CONOCIDOS:")
            for prefijo, datos in patrones.items():
                ejemplos = ", ".join(datos.get("ejemplos", []))
                lineas.append(f"  - Prefijo '{prefijo}'. Ejemplos: {ejemplos}")

        clientes = contexto.get("clientes_conocidos", {})
        if clientes:
            lineas.append("\nCLIENTES RECURRENTES:")
            top_clientes = sorted(clientes.items(), key=lambda item: item[1].get('frecuencia', 0), reverse=True)[:10]
            for nombre, datos in top_clientes:
                divisa = datos.get('divisa_comun', 'N/A')
                lineas.append(f"  - {nombre} (Divisa habitual: {divisa})")

        lineas.append("=== FIN DEL CONTEXTO ===")
        return "\n".join(lineas)

    def actualizar_contexto(self, documento_metadata: Dict[str, Any]) -> None:
        """Procesa un diccionario de resultados consolidados y guarda el estado."""
        ctx = self.obtener_contexto_actual()
        
        ctx.setdefault("estadisticas", {"total_documentos_procesados": 0, "total_folios_extraidos": 0, "total_no_detectados": 0})
        ctx.setdefault("categorias_permitidas", self._estructura_base()["categorias_permitidas"])
        ctx.setdefault("patrones_folio", {})
        ctx.setdefault("clientes_conocidos", {})
        ctx.setdefault("documentos_recientes", [])

        ctx["estadisticas"]["total_documentos_procesados"] += 1
        
        folio = documento_metadata.get("Folio", "")
        cliente_raw = documento_metadata.get("Cliente", "")
        divisa = documento_metadata.get("Divisa", "NO_DETECTADA")

        if folio and not folio.startswith("ERROR") and not folio.startswith("HUERFANO"):
            ctx["estadisticas"]["total_folios_extraidos"] += 1
            
            letra_inicial = ''.join([c for c in folio if c.isalpha()])
            if letra_inicial:
                prefijo = letra_inicial[0].upper()
                if prefijo not in ctx["patrones_folio"]:
                    ctx["patrones_folio"][prefijo] = {"frecuencia": 0, "ejemplos": []}
                
                ctx["patrones_folio"][prefijo]["frecuencia"] += 1
                if folio not in ctx["patrones_folio"][prefijo]["ejemplos"]:
                    ctx["patrones_folio"][prefijo]["ejemplos"].append(folio)
                    ctx["patrones_folio"][prefijo]["ejemplos"] = ctx["patrones_folio"][prefijo]["ejemplos"][-3:]

            if cliente_raw and cliente_raw != "NO DETECTADO":
                cliente = cliente_raw.upper()
                if cliente not in ctx["clientes_conocidos"]:
                    ctx["clientes_conocidos"][cliente] = {"frecuencia": 0, "divisa_comun": divisa}
                ctx["clientes_conocidos"][cliente]["frecuencia"] += 1
                ctx["clientes_conocidos"][cliente]["divisa_comun"] = divisa

            ctx["documentos_recientes"].append({
                "folio": folio,
                "cliente": cliente_raw,
                "divisa": divisa,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            ctx["documentos_recientes"] = ctx["documentos_recientes"][-20:]
        else:
            ctx["estadisticas"]["total_no_detectados"] += 1

        ctx["ultima_actualizacion"] = datetime.now(timezone.utc).isoformat()
        self.storage.save(ctx)

    def _estructura_base(self) -> Dict[str, Any]:
        """Provee un diccionario inmutable con las entidades fundacionales del sistema."""
        now = datetime.now(timezone.utc).isoformat()
        return {
            "version": "2.0",
            "creado": now,
            "ultima_actualizacion": now,
            "estadisticas": {
                "total_documentos_procesados": 0,
                "total_folios_extraidos": 0,
                "total_no_detectados": 0
            },
            "categorias_permitidas": [
                "Factura", "Remisión", "Devolución de ventas", "Devolución de compras",
                "Recepción de mercancía", "Pedido", "Compra", "Orden de compra",
                "Diarios de ventas", "Nota de remisión", "Ticket", "Recibo", "Otro"
            ],
            "patrones_folio": {},
            "clientes_conocidos": {},
            "documentos_recientes": []
        }


def get_context_manager(s3_client: Optional[Any] = None) -> SystemContextManager:
    if settings.execution_mode == "cloud":
        if s3_client is None:
            import boto3
            s3_client = boto3.client("s3")
        strategy = S3StorageStrategy(
            s3_client=s3_client,
            bucket_name=settings.aws_bucket_name,
            object_key="system/context.json"
        )
    else:
        context_path = settings.data_dir / "system_context.json"
        strategy = LocalStorageStrategy(file_path=context_path)
        
    return SystemContextManager(storage_strategy=strategy)