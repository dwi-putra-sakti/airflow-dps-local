from django.db import connection
from typing import List, Dict, Any, Optional


def execute_raw_sql(query: str, params: Optional[list] = None) -> List[Dict[str, Any]]:
    """
    Fungsi reusable untuk eksekusi SQL raw yang bisa digunakan di seluruh project

    Args:
        query: String query SQL
        params: Parameter query (optional)

    Returns:
        List of dictionaries dengan nama kolom sebagai key
    """
    with connection.cursor() as cursor:
        cursor.execute(query, params or [])
        columns = [col[0] for col in cursor.description] if cursor.description else []
        return [dict(zip(columns, row)) for row in cursor.fetchall()]