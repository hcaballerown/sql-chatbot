from botbuilder.core import ActivityHandler, TurnContext, MessageFactory, ConversationState, UserState
from botbuilder.schema import ChannelAccount, Activity, ActivityTypes
import logging
import json
import asyncio
from typing import Dict, List
import os

# Importar nuestro bot logic
from bot_core_logic import SQLChatBot

class SQLQueryBot(ActivityHandler):
    """Bot principal que maneja la comunicación con Slack"""
    
    def __init__(self, conversation_state: ConversationState, user_state: UserState):
        self.conversation_state = conversation_state
        self.user_state = user_state
        self.logger = logging.getLogger(__name__)
        
        # Inicializar el motor SQL
        self.sql_bot = SQLChatBot(
            openai_api_key=os.environ['OPENAI_API_KEY'],
            openai_endpoint=os.environ['OPENAI_ENDPOINT'],
            connection_string=os.environ['DATABASE_CONNECTION_STRING']
        )
        
        # Estado de conversación
        self.user_profile_accessor = self.user_state.create_property("UserProfile")
        self.conversation_data_accessor = self.conversation_state.create_property("ConversationData")

    async def on_message_activity(self, turn_context: TurnContext):
        """Maneja mensajes entrantes"""
        try:
            user_message = turn_context.activity.text.strip()
            user_id = turn_context.activity.from_property.id
            
            self.logger.info(f"Received message from {user_id}: {user_message}")
            
            # Verificar comandos especiales
            if user_message.lower().startswith('/help'):
                await self._send_help_message(turn_context)
                return
            
            if user_message.lower().startswith('/refresh'):
                await self._refresh_schema(turn_context)
                return
            
            if user_message.lower().startswith('/examples'):
                await self._send_examples(turn_context)
                return
            
            # Mostrar indicador de "typing"
            await self._send_typing_indicator(turn_context)
            
            # Procesar pregunta SQL
            result = self.sql_bot.process_user_question(user_message, user_id)
            
            if result['success']:
                await self._send_sql_response(turn_context, result['data'])
            else:
                await self._send_error_response(turn_context, result['error'])
                
        except Exception as e:
            self.logger.error(f"Error in on_message_activity: {e}")
            await self._send_error_response(turn_context, "Lo siento, ocurrió un error procesando tu mensaje.")
        
        # Guardar estado
        await self.conversation_state.save_changes(turn_context)
        await self.user_state.save_changes(turn_context)

    async def on_members_added_activity(self, members_added: List[ChannelAccount], turn_context: TurnContext):
        """Mensaje de bienvenida cuando el bot se agrega a una conversación"""
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await self._send_welcome_message(turn_context)

    async def _send_welcome_message(self, turn_context: TurnContext):
        """Envía mensaje de bienvenida"""
        welcome_text = """
🤖 **¡Hola! Soy tu asistente de consultas SQL**

Puedo ayudarte a consultar la base de datos usando lenguaje natural.

**Ejemplos de preguntas:**
• "Dame todos los clientes con facturas vencidas"
• "¿Cuáles fueron las ventas del mes pasado?"
• "Muéstrame los productos más vendidos"
• "¿Qué facturas están pendientes de pago?"

**Comandos disponibles:**
• `/help` - Ver esta ayuda
• `/examples` - Ver más ejemplos
• `/refresh` - Actualizar esquema de BD

¡Hazme una pregunta sobre los datos!
        """
        
        welcome_card = MessageFactory.text(welcome_text)
        await turn_context.send_activity(welcome_card)

    async def _send_help_message(self, turn_context: TurnContext):
        """Envía mensaje de ayuda"""
        help_text = """
🔍 **Comandos disponibles:**

**Consultas en lenguaje natural:**
• "Dame todos los clientes con facturas vencidas"
• "¿Cuáles fueron las ventas del mes pasado?"
• "Muéstrame los productos más vendidos esta semana"
• "¿Qué facturas están pendientes de pago?"
• "Cuántos pedidos tenemos hoy?"

**Comandos especiales:**
• `/help` - Mostrar esta ayuda
• `/examples` - Ver más ejemplos de consultas
• `/refresh` - Actualizar esquema de base de datos

**Consejos:**
✅ Sé específico en tus preguntas
✅ Puedes preguntar por fechas, rangos, totales
✅ El bot entiende español e inglés
❌ Solo consultas de lectura (SELECT)
        """
        
        await turn_context.send_activity(MessageFactory.text(help_text))

    async def _send_examples(self, turn_context: TurnContext):
        """Envía ejemplos de consultas"""
        examples_text = """
💡 **Ejemplos de consultas que puedes hacer:**

**Ventas y Facturación:**
• "Ventas totales del último trimestre"
• "Facturas emitidas esta semana"
• "Clientes con mayor volumen de compras"
• "Productos con más ingresos este año"

**Clientes:**
• "Nuevos clientes registrados este mes"
• "Clientes que no han comprado en 90 días"
• "Top 10 clientes por volumen de compras"

**Inventario y Productos:**
• "Productos con stock bajo"
• "Artículos más vendidos por categoría"
• "Productos sin ventas en el último mes"

**Análisis Temporal:**
• "Comparar ventas de este mes vs mes anterior"
• "Tendencia de ventas por semana"
• "Picos de ventas por día de la semana"

¡Prueba cualquiera de estas o haz tu propia pregunta!
        """
        
        await turn_context.send_activity(MessageFactory.text(examples_text))

    async def _refresh_schema(self, turn_context: TurnContext):
        """Refresca el esquema de la base de datos"""
        await turn_context.send_activity(MessageFactory.text("🔄 Actualizando esquema de la base de datos..."))
        
        try:
            success = self.sql_bot.refresh_schema()
            if success:
                message = "✅ Esquema actualizado correctamente. Ahora puedo usar cualquier tabla nueva que hayas agregado."
            else:
                message = "❌ Error al actualizar el esquema. Por favor intenta más tarde."
        except Exception as e:
            self.logger.error(f"Error refreshing schema: {e}")
            message = "❌ Error al actualizar el esquema. Revisa los logs para más detalles."
        
        await turn_context.send_activity(MessageFactory.text(message))

    async def _send_sql_response(self, turn_context: TurnContext, response_data: Dict):
        """Envía la respuesta de una consulta SQL exitosa"""
        try:
            # Mensaje principal con los resultados
            main_message = response_data['text']
            await turn_context.send_activity(MessageFactory.text(main_message))
            
            # Información adicional en mensaje separado
            info_text = f"📊 **Detalles de la consulta:**\n"
            info_text += f"• Registros encontrados: {response_data['row_count']}\n"
            info_text += f"• SQL generado: `{response_data['sql_query'][:100]}{'...' if len(response_data['sql_query']) > 100 else ''}`"
            
            await turn_context.send_activity(MessageFactory.text(info_text))
            
        except Exception as e:
            self.logger.error(f"Error sending SQL response: {e}")
            await turn_context.send_activity(MessageFactory.text("Error al formatear la respuesta."))

    async def _send_error_response(self, turn_context: TurnContext, error_message: str):
        """Envía mensaje de error al usuario"""
        error_text = f"❌ **Error:** {error_message}\n\n"
        error_text += "💡 **Sugerencias:**\n"
        error_text += "• Verifica que tu pregunta sea clara\n"
        error_text += "• Intenta reformular la consulta\n"
        error_text += "• Usa `/examples` para ver ejemplos\n"
        error_text += "• Usa `/help` para ver comandos disponibles"
        
        await turn_context.send_activity(MessageFactory.text(error_text))

    async def _send_typing_indicator(self, turn_context: TurnContext):
        """Envía indicador de que el bot está procesando"""
        typing_activity = Activity(
            type=ActivityTypes.typing,
            relates_to=turn_context.activity.relates_to
        )
        await turn_context.send_activity(typing_activity)

# Clase de estado de usuario (opcional para funcionalidades avanzadas)
class UserProfile:
    def __init__(self):
        self.name = None
        self.query_count = 0
        self.last_query_time = None
        self.favorite_queries = []

class ConversationData:
    def __init__(self):
        self.last_query = None
        self.last_results_count = 0
        self.conversation_started = False