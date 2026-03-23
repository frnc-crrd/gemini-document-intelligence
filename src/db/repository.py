"""Patrón Repositorio para aislamiento de operaciones de base de datos.

Maneja el pool de conexiones y las transacciones masivas (Bulk Inserts),
garantizando ACID (Atomicidad, Consistencia, Aislamiento y Durabilidad).
Implementa un coordinador de estado en memoria (Thread-Safe Cache) para 
resolver colisiones de versionamiento y deduplicación pre-transaccional.
"""

import threading
from typing import List, Dict, Any, Tuple
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from src.config import get_settings
from src.core.logger import get_system_logger
from src.db.models import Base, RegistroArtefacto

logger = get_system_logger(__name__)
settings = get_settings()


class PostgresRepository:
    """Gestor transaccional para la base de datos de auditoría (Staging Area)."""

    def __init__(self) -> None:
        self.engine = create_engine(
            settings.database_url,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True
        )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        self._cache_lock = threading.Lock()
        # Modificación: La llave del caché ahora es compuesta (folio, categoria)
        self._version_cache: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

    def initialize_schema(self) -> None:
        """Construye el esquema DDL en el motor PostgreSQL si no existe."""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("Esquema relacional validado/inicializado en PostgreSQL exitosamente.")
        except SQLAlchemyError as e:
            logger.critical(f"Fallo al inicializar el esquema de base de datos: {e}", exc_info=True)
            raise

    def resolve_versioning(self, folio: str, categoria: str, archivo_original: str, confianza: int) -> Tuple[int, str]:
        """Aplica la regla de deduplicación vs versionamiento de manera Thread-Safe.

        Args:
            folio: Identificador extraído del documento.
            categoria: Tipo lógico del documento detectado.
            archivo_original: Nombre del archivo de origen.
            confianza: Nivel de certeza de la extracción.

        Returns:
            Tuple[int, str]: (Versión a asignar, Acción a tomar ['NUEVO', 'SOBRESCRIBIR', 'DESCARTAR']).
        """
        cache_key = (folio, categoria)
        
        with self._cache_lock:
            if cache_key not in self._version_cache:
                with self.SessionLocal() as session:
                    registros = session.query(RegistroArtefacto).filter_by(folio=folio, categoria=categoria).all()
                    self._version_cache[cache_key] = [
                        {"origen": r.archivo_original, "version": r.version, "score": r.confianza_promedio}
                        for r in registros
                    ]
            
            registros_mem = self._version_cache[cache_key]
            
            if not registros_mem:
                self._version_cache[cache_key].append({"origen": archivo_original, "version": 1, "score": confianza})
                return 1, "NUEVO"
            
            for reg in registros_mem:
                if reg["origen"] == archivo_original:
                    if confianza >= reg["score"]:
                        reg["score"] = confianza
                        return reg["version"], "SOBRESCRIBIR"
                    else:
                        return reg["version"], "DESCARTAR"
                        
            max_version = max(r["version"] for r in registros_mem)
            new_version = max_version + 1
            self._version_cache[cache_key].append({"origen": archivo_original, "version": new_version, "score": confianza})
            return new_version, "NUEVO"

    def upsert_batch(self, batch_data: List[Dict[str, Any]]) -> None:
        """Inserta nuevos registros o actualiza los existentes con deduplicación In-Memory."""
        if not batch_data:
            return

        # Deduplicación Pre-Transaccional: Evita colisiones de estado no sincronizado (unflushed)
        deduplicated_batch = {}
        for item in batch_data:
            key = (item.get("Folio"), item.get("Categoría", "No identificado"), item.get("Versión", 1), item.get("Archivo Original"))
            deduplicated_batch[key] = item

        with self.SessionLocal() as session:
            try:
                for item in deduplicated_batch.values():
                    folio = item.get("Folio", "ERROR_SIN_FOLIO")
                    categoria = item.get("Categoría", "No identificado")
                    version = item.get("Versión", 1)
                    archivo_original = item.get("Archivo Original", "DESCONOCIDO")
                    ruta_asignada = item.get("Ruta del Archivo", "")
                    
                    existente = session.query(RegistroArtefacto).filter_by(
                        folio=folio, categoria=categoria, version=version, archivo_original=archivo_original
                    ).first()
                    
                    if existente:
                        existente.divisa = item.get("Divisa", existente.divisa)
                        existente.cliente = item.get("Cliente", existente.cliente)
                        existente.paginas_consolidado = item.get("Páginas Final", existente.paginas_consolidado)
                        existente.status = item.get("Status", existente.status)
                        existente.confianza_promedio = item.get("Confianza", existente.confianza_promedio)
                        existente.ruta_servidor = ruta_asignada
                        existente.justificacion = item.get("Justificación", existente.justificacion)
                    else:
                        record = RegistroArtefacto(
                            folio=folio,
                            categoria=categoria,
                            version=version,
                            divisa=item.get("Divisa", "NO_DETECTADA"),
                            cliente=item.get("Cliente", "NO DETECTADO"),
                            archivo_original=archivo_original,
                            paginas_consolidado=item.get("Páginas Final", 1),
                            status=item.get("Status", "PENDIENTE"),
                            confianza_promedio=item.get("Confianza", 0),
                            ruta_servidor=ruta_asignada,
                            justificacion=item.get("Justificación", "")
                        )
                        session.add(record)
                        
                session.commit()
                logger.info(f"Transacción exitosa: Lote de {len(deduplicated_batch)} artefactos UPSERT en PostgreSQL.")
            except SQLAlchemyError as e:
                session.rollback()
                logger.error(f"Fallo de integridad transaccional al insertar el lote. Rollback ejecutado: {e}", exc_info=True)