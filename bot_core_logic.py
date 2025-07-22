import openai
import pyodbc
import json
import re
import logging
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import pandas as pd

class SQLChatBot:
    def __init__(self, openai_api_key: str, openai_endpoint: str, connection_string: str):
        self.openai_client = openai.AzureOpenAI(
            api_key=openai_api_key,
            api_version="2024-02-15-preview",
            azure_endpoint=openai_endpoint
        )
        self.connection_string = connection_string
        self.logger = logging.getLogger(__name__)
        
        # Cargar el esquema al inicializar
        from schema_discovery import SchemaDiscoveryService
        self.schema_service = SchemaDiscoveryService(connection_string)
        self.schema_prompt = self._load_schema()
        
        # Configuraciones de seguridad
        self.allowed_operations = ['SELECT']
        self.forbidden_keywords = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'TRUNCATE']
    
    def _load_schema(self) -> str:
        """Carga y formatea el esquema de la base de datos"""
        try:
            schema = self.schema_service.get_complete_schema()
            return self.schema_service.generate_schema_prompt(schema)
        except Exception as e:
            self.logger.error(f"Error loading schema: {e}")
            return ""
    
    def process_user_question(self, user_question: str, user_id: str = None) -> Dict:
        """Procesa una pregunta del usuario y retorna la respuesta"""
        try:
            # 1. Validar la pregunta
            if not self._is_valid_question(user_question):
                return {
                    'success': False,
                    'error': 'Pregunta no válida o contiene palabras prohibidas',
                    'data': None
                }
            
            # 2. Generar SQL usando OpenAI
            sql_query = self._generate_sql_query(user_question)
            
            if not sql_query:
                return {
                    'success': False,
                    'error': 'No pude generar una consulta SQL para tu pregunta',
                    'data': None
                }
            
            # 3. Validar el SQL generado
            if not self._validate_sql(sql_query):
                return {
                    'success': False,
                    'error': 'La consulta generada no es segura',
                    'data': None
                }
            
            # 4. Ejecutar la consulta
            results = self._execute_query(sql_query)
            
            # 5. Formatear respuesta
            formatted_response = self._format_response(results, user_question, sql_query)
            
            # 6. Log de auditoría
            self._log_interaction(user_id, user_question, sql_query, len(results) if results else 0)
            
            return {
                'success': True,
                'error': None,
                'data': formatted_response
            }
            
        except Exception as e:
            self.logger.error(f"Error processing question: {e}")
            return {
                'success': False,
                'error': f'Error interno: {str(e)}',
                'data': None
            }
    
    def _generate_sql_query(self, user_question: str) -> Optional[str]:
        """Genera una consulta SQL usando OpenAI"""
        system_prompt = f"""
        Eres un experto en SQL Server especializado en generar consultas precisas y seguras.

        CONTEXTO DE LA BASE DE DATOS:
        {self.schema_prompt}

        REGLAS IMPORTANTES:
        1. SOLO generar consultas SELECT
        2. NO usar funciones peligrosas como xp_cmdshell, OPENROWSET, etc.
        3. Usar JOINs apropiados basándose en las relaciones definidas
        4. Incluir WHERE clauses cuando sea apropiado para limitar resultados
        5. Usar aliases descriptivos para las columnas
        6. Si la pregunta es ambigua, hacer suposiciones razonables
        7. Optimizar para performance cuando sea posible
        8. SIEMPRE incluir TOP 100 para limitar resultados grandes

        FORMATO DE RESPUESTA:
        Responde SOLO con la consulta SQL, sin explicaciones adicionales.
        """
        
        user_prompt = f"""
        Convierte esta pregunta a SQL:
        
        "{user_question}"
        
        Recuerda: Solo la consulta SQL, sin comentarios ni explicaciones.
        """
        
        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-4-32k",  # Usa el modelo que tengas deployado
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,
                max_tokens=1000
            )
            
            sql_query = response.choices[0].message.content.strip()
            
            # Limpiar la respuesta
            sql_query = self._clean_sql_response(sql_query)
            
            self.logger.info(f"Generated SQL: {sql_query}")
            return sql_query
            
        except Exception as e:
            self.logger.error(f"Error generating SQL: {e}")
            return None
    
    def _clean_sql_response(self, sql_query: str) -> str:
        """Limpia la respuesta de OpenAI para extraer solo el SQL"""
        # Remover markdown code blocks
        sql_query = re.sub(r'```sql\n?', '', sql_query)
        sql_query = re.sub(r'```\n?', '', sql_query)
        
        # Remover comentarios de línea
        sql_query = re.sub(r'--.*$', '', sql_query, flags=re.MULTILINE)
        
        # Remover espacios extra y saltos de línea
        sql_query = ' '.join(sql_query.split())
        
        return sql_query.strip()
    
    def _validate_sql(self, sql_query: str) -> bool:
        """Valida que la consulta SQL sea segura"""
        sql_upper = sql_query.upper()
        
        # Verificar que solo contenga SELECT
        if not sql_upper.strip().startswith('SELECT'):
            return False
        
        # Verificar palabras prohibidas
        for keyword in self.forbidden_keywords:
            if keyword in sql_upper:
                return False
        
        # Verificar funciones peligrosas
        dangerous_functions = ['XP_CMDSHELL', 'OPENROWSET', 'OPENDATASOURCE', 'EXEC', 'EXECUTE']
        for func in dangerous_functions:
            if func in sql_upper:
                return False
        
        return True
    
    def _execute_query(self, sql_query: str) -> List[Dict]:
        """Ejecuta la consulta SQL y retorna los resultados"""
        try:
            with pyodbc.connect(self.connection_string) as conn:
                # Configurar timeout
                conn.timeout = 30
                
                df = pd.read_sql(sql_query, conn)
                
                # Convertir a lista de diccionarios
                results = df.to_dict('records')
                
                # Limitar número de resultados
                if len(results) > 100:
                    results = results[:100]
                    self.logger.warning(f"Results truncated to 100 rows")
                
                return results
                
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            raise
    
    def _format_response(self, results: List[Dict], original_question: str, sql_query: str) -> Dict:
        """Formatea la respuesta para Slack"""
        if not results:
            return {
                'text': 'No encontré resultados para tu consulta.',
                'sql_query': sql_query,
                'row_count': 0
            }
        
        response = {
            'text': self._create_slack_table(results, original_question),
            'sql_query': sql_query,
            'row_count': len(results)
        }
        
        return response
    
    def _create_slack_table(self, results: List[Dict], question: str) -> str:
        """Crea una tabla formateada para Slack"""
        if not results:
            return "No hay datos para mostrar."
        
        # Título
        response = f"*Resultados para: {question}*\n\n"
        
        # Si hay muchas columnas, mostrar solo las primeras
        first_row = results[0]
        columns = list(first_row.keys())
        
        if len(columns) > 5:
            # Mostrar resumen en lugar de tabla completa
            response += f"```\nEncontré {len(results)} resultados con {len(columns)} columnas.\n"
            response += f"Primeras columnas: {', '.join(columns[:5])}\n"
            response += f"Muestra de los primeros 3 registros:\n\n"
            
            for i, row in enumerate(results[:3]):
                response += f"Registro {i+1}:\n"
                for col in columns[:5]:
                    value = str(row[col]) if row[col] is not None else 'NULL'
                    response += f"  {col}: {value}\n"
                response += "\n"
            response += "```"
        else:
            # Crear tabla simple
            response += "```\n"
            
            # Headers
            header = " | ".join([str(col)[:15] for col in columns])
            response += header + "\n"
            response += "-" * len(header) + "\n"
            
            # Rows (máximo 10 para que sea legible)
            for row in results[:10]:
                row_str = " | ".join([str(row[col])[:15] if row[col] is not None else 'NULL' for col in columns])
                response += row_str + "\n"
            
            if len(results) > 10:
                response += f"... y {len(results) - 10} registros más\n"
            
            response += "```"
        
        response += f"\n*Total de registros: {len(results)}*"
        
        return response
    
    def _is_valid_question(self, question: str) -> bool:
        """Valida que la pregunta sea apropiada"""
        question_upper = question.upper()
        
        # Verificar palabras prohibidas en la pregunta
        forbidden_in_question = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE']
        for word in forbidden_in_question:
            if word in question_upper:
                return False
        
        # Verificar longitud mínima
        if len(question.strip()) < 5:
            return False
        
        return True
    
    def _log_interaction(self, user_id: str, question: str, sql_query: str, result_count: int):
        """Log de auditoría para todas las interacciones"""
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'user_id': user_id,
            'question': question,
            'sql_query': sql_query,
            'result_count': result_count
        }
        
        # En producción, esto debería ir a Application Insights o una tabla de log
        self.logger.info(f"User interaction: {json.dumps(log_entry)}")
    
    def refresh_schema(self):
        """Refresca el esquema de la base de datos"""
        try:
            self.schema_prompt = self._load_schema()
            self.logger.info("Schema refreshed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error refreshing schema: {e}")
            return False

# Función helper para Azure Function
def process_slack_message(message_text: str, user_id: str) -> Dict:
    """Función principal para procesar mensajes de Slack"""
    import os
    
    # Configuración desde variables de entorno
    openai_key = os.environ['OPENAI_API_KEY']
    openai_endpoint = os.environ['OPENAI_ENDPOINT']
    db_connection = os.environ['DATABASE_CONNECTION_STRING']
    
    # Inicializar bot
    bot = SQLChatBot(openai_key, openai_endpoint, db_connection)
    
    # Procesar pregunta
    result = bot.process_user_question(message_text, user_id)
    
    return result

# Ejemplo de uso
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Configuración de prueba
    bot = SQLChatBot(
        openai_api_key="your-key",
        openai_endpoint="your-endpoint", 
        connection_string="your-connection-string"
    )
    
    # Prueba
    test_questions = [
        "Dame todos los clientes con facturas vencidas",
        "¿Cuáles son las ventas del mes pasado?",
        "Muéstrame los productos más vendidos"
    ]
    
    for question in test_questions:
        print(f"\nPregunta: {question}")
        result = bot.process_user_question(question, "test_user")
        print(f"Resultado: {result}")