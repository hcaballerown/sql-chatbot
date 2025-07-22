import pyodbc
import json
from datetime import datetime
import logging

class SchemaDiscoveryService:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.logger = logging.getLogger(__name__)
    
    def get_complete_schema(self):
        """Obtiene el esquema completo de la base de datos"""
        try:
            with pyodbc.connect(self.connection_string) as conn:
                cursor = conn.cursor()
                
                schema = {
                    'tables': self._get_tables_info(cursor),
                    'relationships': self._get_foreign_keys(cursor),
                    'indexes': self._get_indexes(cursor),
                    'views': self._get_views(cursor),
                    'last_updated': datetime.utcnow().isoformat()
                }
                
                return schema
        except Exception as e:
            self.logger.error(f"Error getting schema: {e}")
            raise
    
    def _get_tables_info(self, cursor):
        """Obtiene información detallada de todas las tablas"""
        query = """
        SELECT 
            t.TABLE_SCHEMA,
            t.TABLE_NAME,
            t.TABLE_TYPE,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as IS_PRIMARY_KEY,
            ep.value as COLUMN_DESCRIPTION
        FROM INFORMATION_SCHEMA.TABLES t
        JOIN INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
        LEFT JOIN (
            SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
                ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
                AND tc.TABLE_NAME = ku.TABLE_NAME
        ) pk ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA 
              AND c.TABLE_NAME = pk.TABLE_NAME 
              AND c.COLUMN_NAME = pk.COLUMN_NAME
        LEFT JOIN sys.extended_properties ep ON ep.major_id = OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME)
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        tables = {}
        for row in results:
            schema_name = row[0]
            table_name = row[1]
            table_key = f"{schema_name}.{table_name}"
            
            if table_key not in tables:
                tables[table_key] = {
                    'schema': schema_name,
                    'name': table_name,
                    'type': row[2],
                    'columns': []
                }
            
            column_info = {
                'name': row[3],
                'data_type': row[4],
                'is_nullable': row[5] == 'YES',
                'default_value': row[6],
                'max_length': row[7],
                'precision': row[8],
                'scale': row[9],
                'is_primary_key': bool(row[10]),
                'description': row[11]
            }
            
            tables[table_key]['columns'].append(column_info)
        
        return tables
    
    def _get_foreign_keys(self, cursor):
        """Obtiene todas las relaciones de foreign keys"""
        query = """
        SELECT 
            fk.name AS FK_NAME,
            tp.name AS PARENT_TABLE,
            cp.name AS PARENT_COLUMN,
            tr.name AS REFERENCED_TABLE,
            cr.name AS REFERENCED_COLUMN,
            fk.delete_referential_action_desc,
            fk.update_referential_action_desc
        FROM sys.foreign_keys fk
        INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        INNER JOIN sys.tables tp ON fkc.parent_object_id = tp.object_id
        INNER JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
        INNER JOIN sys.tables tr ON fkc.referenced_object_id = tr.object_id
        INNER JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
        ORDER BY tp.name, fk.name
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        relationships = []
        for row in results:
            relationships.append({
                'constraint_name': row[0],
                'parent_table': row[1],
                'parent_column': row[2],
                'referenced_table': row[3],
                'referenced_column': row[4],
                'delete_action': row[5],
                'update_action': row[6]
            })
        
        return relationships
    
    def _get_indexes(self, cursor):
        """Obtiene información de índices para optimización de consultas"""
        query = """
        SELECT 
            t.name AS TABLE_NAME,
            i.name AS INDEX_NAME,
            i.type_desc AS INDEX_TYPE,
            i.is_unique,
            i.is_primary_key,
            STRING_AGG(c.name, ', ') AS COLUMNS
        FROM sys.indexes i
        INNER JOIN sys.tables t ON i.object_id = t.object_id
        INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.type > 0  -- Exclude heaps
        GROUP BY t.name, i.name, i.type_desc, i.is_unique, i.is_primary_key
        ORDER BY t.name, i.name
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        indexes = []
        for row in results:
            indexes.append({
                'table_name': row[0],
                'index_name': row[1],
                'index_type': row[2],
                'is_unique': bool(row[3]),
                'is_primary_key': bool(row[4]),
                'columns': row[5]
            })
        
        return indexes
    
    def _get_views(self, cursor):
        """Obtiene información de vistas para consultas complejas"""
        query = """
        SELECT 
            TABLE_SCHEMA,
            TABLE_NAME,
            VIEW_DEFINITION
        FROM INFORMATION_SCHEMA.VIEWS
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        views = []
        for row in results:
            views.append({
                'schema': row[0],
                'name': row[1],
                'definition': row[2]
            })
        
        return views
    
    def generate_schema_prompt(self, schema):
        """Genera un prompt optimizado para OpenAI con el esquema"""
        prompt = "# Base de Datos - Esquema Completo\n\n"
        prompt += "## Tablas y Columnas:\n"
        
        for table_key, table in schema['tables'].items():
            prompt += f"\n### {table_key}\n"
            prompt += "| Columna | Tipo | Nullable | PK | Descripción |\n"
            prompt += "|---------|------|----------|----|--------------|\n"
            
            for col in table['columns']:
                pk_marker = "✓" if col['is_primary_key'] else ""
                nullable = "Sí" if col['is_nullable'] else "No"
                desc = col['description'] or ""
                prompt += f"| {col['name']} | {col['data_type']} | {nullable} | {pk_marker} | {desc} |\n"
        
        prompt += "\n## Relaciones (Foreign Keys):\n"
        for rel in schema['relationships']:
            prompt += f"- {rel['parent_table']}.{rel['parent_column']} → {rel['referenced_table']}.{rel['referenced_column']}\n"
        
        prompt += "\n## Vistas Disponibles:\n"
        for view in schema['views']:
            prompt += f"- {view['schema']}.{view['name']}\n"
        
        return prompt

# Función para usar en Azure Function
def get_schema_for_openai(connection_string):
    """Función principal para obtener esquema formateado para OpenAI"""
    service = SchemaDiscoveryService(connection_string)
    schema = service.get_complete_schema()
    return service.generate_schema_prompt(schema)

# Ejemplo de uso
if __name__ == "__main__":
    # Configurar logging
    logging.basicConfig(level=logging.INFO)
    
    # String de conexión (usar variables de entorno en producción)
    conn_str = "Driver={ODBC Driver 17 for SQL Server};Server=your-server.database.windows.net;Database=your-db;Uid=your-user;Pwd=your-password;Encrypt=yes;TrustServerCertificate=no;"
    
    try:
        service = SchemaDiscoveryService(conn_str)
        schema = service.get_complete_schema()
        
        # Guardar esquema en archivo para debugging
        with open('schema.json', 'w', encoding='utf-8') as f:
            json.dump(schema, f, indent=2, ensure_ascii=False)
        
        # Generar prompt para OpenAI
        prompt = service.generate_schema_prompt(schema)
        print("Schema prompt generado exitosamente")
        print(f"Prompt length: {len(prompt)} characters")
        
    except Exception as e:
        print(f"Error: {e}")