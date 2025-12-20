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

ALVARO_GOMEZ_RUEDA    = "1420090507"
WELLINGTON_BRITO      = "1168182475"
JONATAS_TOMAZ         = "1428232020"
NICOLE_D_AMBROSI      = "1197681528"
ANSELMO_BENTO         = "1466207452"
FLAVIO_MOREIRA_JUNIOR = "1147358291"
GUSTAVO_ARAUJO        = "1394913806"
CARLA_DE_CARLO        = "1419866553"
LEONARDO_CURYLOFO     = "1168404041"
MARCELO_GEORGETE      = "9461760940"
ERIVANDO_ALVES        = "1193239865"
JURACI_JUNIOR         = "1508061048"
AMANDA_RIBEIRO        = "9168146748"
BIANCA_SILVA          = "1189409534"
FABRICIO_CRUZ         = "" 
IROMAR_SOUZA          = "1461929762"
DENER_QUIRINO         = "9327754351"
DOUGLAS_FIALHO        = "1440989413"
TABATA_ADAO           = "1415803050"
MARIANE_MARQUEZINI    = "9260655622"
IRAN_CASTRO           = "1361341535"
EDER_SILVA            = "1369730712"
DANILO_PEREIRA        = "1210347148"
WILLIAN_SANTOS        = "1273261718"
ENEIAS_ALVES          = "1424247344"
FELIPE_BATISTA        = "1277449046"
LUCAS_SALOME          = "1248089873"
BRUNO_PAULO           = "1461934187"
FABIA_PRESTES         = "1449337032"
ANDERSON_OLIVEIRA     = "9520696251"
FABRICIO_DAMASCENO    = "9356934188"
FERNANDO_COSTA        = "9289770437"
RODRIGO_DONIZETTI     = "9507928603"
ALEX_RODRIGUES        = "1474710540"
CARLOS_CESAR_BIANCHINI= "1369817027"
SYLVIO_NETTO          = "1151848215"
MARCELO_LUNADERLO     = "9184928869"
DANIELA_BRAZ          = "1453743924"
CARLOS_OLIVEIRA       = "1172690482"

# ==============================================================================
# 📢 LISTA DE NOTIFICAÇÃO UNIFICADA
# ==============================================================================
LISTA_BRUTA = [
    ALVARO_GOMEZ_RUEDA, WELLINGTON_BRITO, JONATAS_TOMAZ, NICOLE_D_AMBROSI,
    ANSELMO_BENTO, FLAVIO_MOREIRA_JUNIOR, GUSTAVO_ARAUJO, CARLA_DE_CARLO,
    LEONARDO_CURYLOFO, MARCELO_GEORGETE, ERIVANDO_ALVES, JURACI_JUNIOR,
    AMANDA_RIBEIRO, BIANCA_SILVA, FABRICIO_CRUZ, IROMAR_SOUZA,
    DENER_QUIRINO, DOUGLAS_FIALHO, TABATA_ADAO, MARIANE_MARQUEZINI,
    IRAN_CASTRO, EDER_SILVA, DANILO_PEREIRA, WILLIAN_SANTOS,
    ENEIAS_ALVES, FELIPE_BATISTA, LUCAS_SALOME, BRUNO_PAULO,
    FABIA_PRESTES, ANDERSON_OLIVEIRA, FABRICIO_DAMASCENO, FERNANDO_COSTA,
    RODRIGO_DONIZETTI, ALEX_RODRIGUES, CARLOS_CESAR_BIANCHINI, SYLVIO_NETTO,
    MARCELO_LUNADERLO, DANIELA_BRAZ, CARLOS_OLIVEIRA
]
EQUIPE_COMPLETA = [uid for uid in LISTA_BRUTA if uid]

# --- AUTENTICAÇÃO SEGURA ---
def autenticar_google(creds_var):
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
    # --- DIAGNÓSTICO DE AMBIENTE ---
    print("\n" + "="*40)
    print("🕵️ INICIANDO DIAGNÓSTICO DE SEGREDOS")
    print("="*40)
    
    # 1. Recupera Variáveis
    webhook_url = os.environ.get('WEBHOOK_URL') or os.environ.get('SEATALK_WEBHOOK_URL')
    spreadsheet_id = os.environ.get('SHEET_ID') or os.environ.get('SPREADSHEET_ID')
    creds_var = os.environ.get('GSPREAD_CREDENTIALS') or os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')

    # 2. Verifica WEBHOOK
    if webhook_url:
        print(f"✅ WEBHOOK_URL: Encontrado (Tamanho: {len(webhook_url)} caracteres)")
    else:
        print("❌ WEBHOOK_URL: NÃO ENCONTRADO! Verifique o nome do segredo no GitHub.")

    # 3. Verifica SHEET_ID
    if spreadsheet_id:
        print(f"✅ SHEET_ID: Encontrado (Valor: {spreadsheet_id})")
        if "google.com" in spreadsheet_id:
            print("⚠️ AVISO: Parece que você colou o LINK inteiro no SHEET_ID. Use apenas o código ID.")
    else:
        print("❌ SHEET_ID: NÃO ENCONTRADO! Verifique o nome do segredo no GitHub.")

    # 4. Verifica CREDENCIAIS
    if creds_var:
        print(f"✅ GSPREAD_CREDENTIALS: Encontrado (Tamanho: {len(creds_var)} caracteres)")
    else:
        print("❌ GSPREAD_CREDENTIALS: NÃO ENCONTRADO! Verifique o nome do segredo no GitHub.")
    
    print("="*40 + "\n")

    # Se faltar algo crítico, encerra aqui
    if not webhook_url or not spreadsheet_id or not creds_var:
        print("⛔ EXECUÇÃO INTERROMPIDA POR FALTA DE CONFIGURAÇÃO.")
        return

    # --- EXECUÇÃO NORMAL ---
    cliente = autenticar_google(creds_var)
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
