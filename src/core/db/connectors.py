from django.db import connections
from typing import Dict, Any, List, Optional


class DatabaseConnector:
    """Handler untuk multiple database connection"""

    @staticmethod
    def execute_raw_sql(
            query: str,
            params: Optional[list] = None,
            using: str = 'default',
            return_dict: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Eksekusi raw SQL ke database tertentu

        Args:
            query: Query SQL
            params: Parameter query
            using: Nama database (sesuai config di settings.py)
            return_dict: Return sebagai dictionary atau raw tuple

        Returns:
            Hasil query dalam format yang diminta
        """
        with connections[using].cursor() as cursor:
            cursor.execute(query, params or [])

            if not return_dict:
                return cursor.fetchall()

            columns = [col[0] for col in cursor.description] if cursor.description else []
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    @staticmethod
    def get_databases() -> List[str]:
        """Daftar semua database yang terkonfigurasi"""
        return list(connections.databases.keys())