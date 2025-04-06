import asyncpg
from typing import Any, List, Dict, Optional


class DBUtils:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        """Устанавливает соединение с базой данных."""
        if not self.pool:
            self.pool = await asyncpg.create_pool(self.database_url)
            print("Connected to the database.")

    async def close(self) -> None:
        """Закрывает соединение с базой данных."""
        if self.pool:
            await self.pool.close()
            print("Database connection closed.")

    async def execute(self, query: str, *args: Any) -> None:
        """Выполняет SQL-запрос без возврата результатов."""
        async with self.pool.acquire() as conn:
            await conn.execute(query, *args)

    async def fetch(self, query: str, *args: Any) -> List[Dict[str, Any]]:
        """Выполняет SQL-запрос и возвращает список словарей."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
            return [dict(row) for row in rows]

    async def fetchval(self, query: str, *args: Any) -> Any:
        """Выполняет SQL-запрос и возвращает одно значение."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def fetchrow(self, query: str, *args: Any) -> Optional[Dict[str, Any]]:
        """Выполняет SQL-запрос и возвращает одну строку в виде словаря."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, *args)
            return dict(row) if row else None