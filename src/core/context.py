"""Gestor de contexto universal (Local / Cloud).

Implementa el Patrón de Estrategia para persistir la memoria global del sistema.
Gestiona el catálogo dinámico de tipos de documento.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from src import config
from src.models import LogicalDocument


class SystemContextManager:
    """Gestiona la memoria colectiva del sistema adaptándose al entorno de ejecución."""

    def __init__(self):
        self.mode = config.EXECUTION_MODE
        
        if self.mode == "local":
            self.context_path = config.DATA_DIR / "system_context.json"
        elif self.mode == "cloud":
            import boto3
            self.s3_client = boto3.client('s3')
            self.bucket_name = config.AWS_BUCKET_NAME
            self.context_key = "system/context.json"
        else:
            raise ValueError(f"Modo de ejecución no soportado: {self.mode}")

    def obtener_contexto_actual(self) -> Dict[str, Any]:
        """Recupera el estado global actual del sistema según el entorno."""
        try:
            if self.mode == "local":
                if self.context_path.exists():
                    with open(self.context_path, 'r', encoding='utf-8') as f:
                        return json.load(f)
            
            elif self.mode == "cloud":
                response = self.s3_client.get_object(
                    Bucket=self.bucket_name, 
                    Key=self.context_key
                )
                return json.loads(response['Body'].read().decode('utf-8'))
        except Exception:
            pass
            
        return self._estructura_base()

    def generar_prompt_contexto(self) -> str:
        """Construye el string de contexto histórico para inyectar en Gemini."""
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
                lineas.append(f"  - Prefijo '{prefijo}': Asociado a {datos.get('tipo_asociado')}. Ejemplos: {ejemplos}")

        clientes = contexto.get("clientes_conocidos", {})
        if clientes:
            lineas.append("\nCLIENTES RECURRENTES:")
            top_clientes = sorted(clientes.items(), key=lambda x: x[1].get('frecuencia', 0), reverse=True)[:10]
            for nombre, datos in top_clientes:
                lineas.append(f"  - {nombre}")

        lineas.append("=== FIN DEL CONTEXTO ===")
        return "\n".join(lineas)

    def actualizar_contexto(self, documento: LogicalDocument) -> None:
        """Actualiza la memoria global y expande el catálogo de categorías si es necesario."""
        ctx = self.obtener_contexto_actual()
        
        ctx["estadisticas"]["total_documentos_procesados"] += 1
        
        tipo_limpio = documento.document_type.strip().capitalize()
        if tipo_limpio not in ctx["categorias_permitidas"]:
            ctx["categorias_permitidas"].append(tipo_limpio)

        for folio in documento.folios:
            if folio and not folio.startswith("ERROR"):
                ctx["estadisticas"]["total_folios_extraidos"] += 1
                
                letra_inicial = ''.join([c for c in folio if c.isalpha()])
                if letra_inicial:
                    prefijo = letra_inicial[0].upper()
                    if prefijo not in ctx["patrones_folio"]:
                        ctx["patrones_folio"][prefijo] = {"tipo_asociado": tipo_limpio, "frecuencia": 0, "ejemplos": []}
                    
                    ctx["patrones_folio"][prefijo]["frecuencia"] += 1
                    if folio not in ctx["patrones_folio"][prefijo]["ejemplos"]:
                        ctx["patrones_folio"][prefijo]["ejemplos"].append(folio)
                        ctx["patrones_folio"][prefijo]["ejemplos"] = ctx["patrones_folio"][prefijo]["ejemplos"][-3:]

                if documento.client_name:
                    cliente = documento.client_name.upper()
                    if cliente not in ctx["clientes_conocidos"]:
                        ctx["clientes_conocidos"][cliente] = {"variantes": [], "frecuencia": 0}
                    ctx["clientes_conocidos"][cliente]["frecuencia"] += 1

                ctx["documentos_recientes"].append({
                    "tipo": tipo_limpio,
                    "folio": folio,
                    "cliente": documento.client_name,
                    "timestamp": datetime.now().isoformat()
                })
                ctx["documentos_recientes"] = ctx["documentos_recientes"][-20:]
            else:
                ctx["estadisticas"]["total_no_detectados"] += 1

        ctx["ultima_actualizacion"] = datetime.now().isoformat()
        self._guardar_contexto(ctx)

    def _guardar_contexto(self, ctx: Dict[str, Any]) -> None:
        if self.mode == "local":
            with open(self.context_path, 'w', encoding='utf-8') as f:
                json.dump(ctx, f, indent=2, ensure_ascii=False)
        elif self.mode == "cloud":
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.context_key,
                Body=json.dumps(ctx, indent=2, ensure_ascii=False)
            )

    def _estructura_base(self) -> Dict[str, Any]:
        """Retorna la plantilla inicial con tus categorías fundacionales."""
        return {
            "version": "1.0",
            "creado": datetime.now().isoformat(),
            "ultima_actualizacion": datetime.now().isoformat(),
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