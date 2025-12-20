import pandas as pd
import gspread
import requests
import base64
import binascii
from datetime import datetime
from pytz import timezone
import os
import json
import sys

# --- CONSTANTES E CONFIGURAÇÕES ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
NOME_ABA = 'Reporte'
FUSO_HORARIO_SP = timezone('America/Sao_Paulo')

# ==============================================================================
# 👥 CADASTRO DA EQUIPE (IDs DE QUEM RECEBE O ALERTA)
# ==============================================================================

IROMAR_SOUZA       = "1461929762"
MURILO_SANTANA     = "1386559133"

# ==============================================================================
# 📢 LISTA DE NOTIFICAÇÃO UNIFICADA
# ==============================================================================
EQUIPE_COMPLETA = [
    IROMAR_SOUZA, MURILO_SANTANA
]

# --- AUTENTICAÇÃO SEGURA ---
def autenticar_google():
    creds_var = os.environ.get('GSPREAD_CREDENTIALS') or os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
    
    if not creds_var:
        print("❌ Erro: Credenciais não encontradas.")
        return None

    creds_dict = None
    try:
        creds_dict = json.loads(creds_var)
    except json.JSONDecodeError:
        try:
            print("🔐 Detectado formato codificado. Decodificando Base64...")
            decoded_bytes = base64.b64decode(creds_var, validate=True)
            decoded_str = decoded_bytes.decode("utf-8")
            creds_dict = json.loads(decoded_str)
        except Exception as e:
            print(f"❌ Erro Crítico nas credenciais: {e}")
            return None

    try:
        return gspread.service_account_from_dict(creds_dict, scopes=SCOPES)
    except Exception as e:
        print(f"❌ Erro ao autenticar no Google: {e}")
        return None

# --- LÓGICA DO SAFETY WALK ---
def buscar_pendencias_safety_walk(cliente, spreadsheet_id):
    if not cliente: return None, "Cliente não conectado."

    try:
        sheet = cliente.open_by_key(spreadsheet_id)
        aba = sheet.worksheet(NOME_ABA)
        todos_dados = aba.get_all_values()
    except Exception as e:
        return None, f"Erro ao abrir aba '{NOME_ABA}': {e}"

    if not todos_dados: return None, "Aba vazia."

    header_nomes = todos_dados[0] 
    dados_operacionais = todos_dados[3:]

    hoje = datetime.now(FUSO_HORARIO_SP).date()

    linha_encontrada = None
    texto_semana = ""
    data_limite_str = ""

    for linha in dados_operacionais:
        try:
            if not linha[0] or not linha[1]: continue
            dt_inicio = datetime.strptime(linha[0], "%d/%m/%Y").date()
            dt_fim = datetime.strptime(linha[1], "%d/%m/%Y").date()
            
            if dt_inicio <= hoje <= dt_fim:
                linha_encontrada = linha
                texto_semana = linha[8]
                data_limite_str = dt_fim.strftime("%d/%m")
                break
        except ValueError:
            continue

    if not linha_encontrada:
        return None, f"Nenhum registro encontrado para a data {hoje.strftime('%d/%m/%Y')}."

    pendencias = []
    for i in range(9, len(linha_encontrada)):
        if i >= len(header_nomes): break 
        status = linha_encontrada[i]
        nome_lider = header_nomes[i]
        if status.strip().upper() == "NÃO REALIZADO":
            pendencias.append(f"❌ {nome_lider}")

    return {
        "semana": texto_semana,
        "data_limite": data_limite_str,
        "lista": pendencias,
        "qtd": len(pendencias)
    }, None

# --- ENVIO WEBHOOK ---
def enviar_webhook(mensagem, webhook_url, user_ids=None):
    if not webhook_url: 
        print("❌ URL do Webhook não definida.")
        return

    payload = {
        "tag": "text",
        "text": { "format": 1, "content": mensagem }
    }
    if user_ids:
        payload["text"]["mentioned_list"] = user_ids

    try:
        req = requests.post(webhook_url, json=payload)
        req.raise_for_status()
        print("✅ Webhook enviado com sucesso.")
    except Exception as e:
        print(f"❌ Erro ao enviar Webhook: {e}")

# --- MAIN ---
def main():
    webhook_url = os.environ.get('WEBHOOK_URL') or os.environ.get('SEATALK_WEBHOOK_URL')
    spreadsheet_id = os.environ.get('SHEET_ID') or os.environ.get('SPREADSHEET_ID')

    if not webhook_url or not spreadsheet_id:
        print("❌ Configurações ausentes.")
        return

    cliente = autenticar_google()
    if not cliente: return

    print("🔎 Verificando planilha 'Reporte'...")
    resultado, erro = buscar_pendencias_safety_walk(cliente, spreadsheet_id)

    if erro:
        print(f"ℹ️ {erro}")
        return

    if resultado and resultado['qtd'] > 0:
        lista_formatada = "\n".join(resultado['lista'])
        mensagem_final = (
            f"⚠️ **Safety Walk Pendente**\n\n"
            f"📅 Período: {resultado['semana']}\n"
            f"❗ {resultado['qtd']} nomes não realizaram:\n\n"
            f"{lista_formatada}\n\n"
            f"Por favor, regularizar até o dia {resultado['data_limite']}!"
        )
        print(f"🚀 Enviando alerta para {len(EQUIPE_COMPLETA)} pessoas...")
        enviar_webhook(mensagem_final, webhook_url, user_ids=EQUIPE_COMPLETA)
    else:
        print("✅ Tudo certo! Nenhuma pendência encontrada.")

if __name__ == "__main__":
    main()
