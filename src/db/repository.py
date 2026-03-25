"""Patrón Repositorio para aislamiento de operaciones de base de datos.

Maneja el pool de conexiones y las transacciones masivas (Bulk Inserts),
garantizando ACID (Atomicidad, Consistencia, Aislamiento y Durabilidad).
Implementa un coordinador de estado en memoria (Thread-Safe Cache) para 
resolver colisiones de versionamiento y delegación vectorizada a PostgreSQL
para deduplicación pre-transaccional.
"""

import threading
from typing import List, Dict, Any, Tuple
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert

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
        """Aplica la regla de deduplicación vs versionamiento de manera Thread-Safe."""
        cache_key = (folio, categoria)
        
        with self._cache_lock:
            if cache_key not in self._version_cache:
                with self.SessionLocal() as session:
                    registros = session.query(RegistroArtefacto).filter_by(folio=folio, categoria=categoria).all()
                    self._version_cache[cache_key] = [
                        # Saneamiento estricto contra valores NULL en base de datos
                        {"origen": r.archivo_original, "version": r.version, "score": r.confianza_promedio or 0}
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
            return new_version, "VERSIONAR"

    def upsert_batch(self, batch_data: List[Dict[str, Any]]) -> None:
        """Delega la inserción masiva y resolución de conflictos al motor de PostgreSQL."""
        if not batch_data:
            return

        deduplicated_batch = {}
        for item in batch_data:
            key = (item.get("Folio"), item.get("Categoría", "No identificado"), item.get("Versión", 1), item.get("Archivo Original"))
            deduplicated_batch[key] = item

        values_to_insert = []
        for item in deduplicated_batch.values():
            values_to_insert.append({
                "folio": item.get("Folio", "ERROR_SIN_FOLIO"),
                "categoria": item.get("Categoría", "No identificado"),
                "version": item.get("Versión", 1),
                "archivo_original": item.get("Archivo Original", "DESCONOCIDO"),
                "divisa": item.get("Divisa", "NO_DETECTADA"),
                "cliente": item.get("Cliente", "NO DETECTADO"),
                "paginas_consolidado": item.get("Páginas Final", 1),
                "status": item.get("Status", "PENDIENTE"),
                "confianza_promedio": item.get("Confianza", 0),
                "ruta_servidor": item.get("Ruta del Archivo", "ERROR_RUTA_VACIA")
            })

        with self.SessionLocal() as session:
            try:
                stmt = insert(RegistroArtefacto).values(values_to_insert)
                
                update_dict = {
                    "divisa": stmt.excluded.divisa,
                    "cliente": stmt.excluded.cliente,
                    "paginas_consolidado": stmt.excluded.paginas_consolidado,
                    "status": stmt.excluded.status,
                    "confianza_promedio": stmt.excluded.confianza_promedio,
                    "ruta_servidor": stmt.excluded.ruta_servidor
                }
                
                stmt = stmt.on_conflict_do_update(
                    index_elements=['folio', 'categoria', 'version', 'archivo_original'],
                    set_=update_dict
                )
                
                session.execute(stmt)
                session.commit()
                logger.info(f"Transacción exitosa: Lote de {len(values_to_insert)} artefactos UPSERT vectorizado en PostgreSQL.")
            except SQLAlchemyError as e:
                session.rollback()
                logger.error(f"Fallo de integridad transaccional al insertar el lote vectorizado. Rollback ejecutado: {e}", exc_info=True)
                raise