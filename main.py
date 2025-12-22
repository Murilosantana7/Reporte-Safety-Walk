import pandas as pd
import gspread
import requests
import base64
import re
from datetime import datetime
from pytz import timezone
import os
import json

# --- CONSTANTES ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
NOME_ABA = 'Reporte'
FUSO_HORARIO_SP = timezone('America/Sao_Paulo')

# ==============================================================================
# 👥 CADASTRO E MAPEAMENTO DA EQUIPE (Nome na Planilha : ID SeaTalk)
# ==============================================================================
# Certifique-se de que os nomes abaixo sejam IGUAIS aos cabeçalhos da planilha
MAPEAMENTO_EQUIPE = {
    "WELLINGTON BRITO": "1168182475",
    "JONATAS TOMAZ": "1428232020",
    "NICOLE D AMBROSI": "1197681528",
    "ANSELMO BENTO": "1466207452",
    "FLAVIO MOREIRA JUNIOR": "1147358291",
    "GUSTAVO ARAUJO": "1394913806",
    "CARLA DE CARLO": "1419866553",
    "LEONARDO CURYLOFO": "1168404041",
    "MARCELO GEORGETE": "9461760940",
    "ERIVANDO ALVES": "1193239865",
    "JURACI JUNIOR": "1508061048",
    "AMANDA RIBEIRO": "9168146748",
    "BIANCA SILVA": "1189409534",
    "FABRICIO CRUZ": "1504109523",
    "IROMAR SOUZA": "1461929762",
    "DENER QUIRINO": "9327754351",
    "DOUGLAS FIALHO": "1440989413",
    "TABATA ADAO": "1415803050",
    "MARIANE MARQUEZINI": "9260655622",
    "IRAN CASTRO": "1361341535",
    "EDER SILVA": "1369730712",
    "DANILO PEREIRA": "1210347148",
    "WILLIAN SANTOS": "1273261718",
    "ENEIAS ALVES": "1424247344",
    "FELIPE BATISTA": "1277449046",
    "LUCAS SALOME": "1248089873",
    "BRUNO PAULO": "1461934187",
    "FABIA PRESTES": "1449337032",
    "ANDERSON OLIVEIRA": "9520696251",
    "FABRICIO DAMASCENO": "9356934188",
    "FERNANDO COSTA": "9289770437",
    "RODRIGO DONIZETTI": "9507928603",
    "ALEX RODRIGUES": "1474710540",
    "CARLOS CESAR BIANCHINI": "1369817027",
    "SYLVIO NETTO": "1151848215",
    "MARCELO LUNADERLO": "9184928869",
    "DANIELA BRAZ": "1453743924",
    "CARLOS OLIVEIRA": "1172690482",
    "ALYSON CAETANO": "1525204706",
    "Edilene Augusto": "1185463777"

}

# --- AUTENTICAÇÃO ---
def autenticar_google(creds_var):
    try:
        # Tenta carregar JSON direto ou decodificar Base64
        try:
            creds_dict = json.loads(creds_var)
        except json.JSONDecodeError:
            decoded_bytes = base64.b64decode(creds_var, validate=True)
            creds_dict = json.loads(decoded_bytes.decode("utf-8"))
        
        return gspread.service_account_from_dict(creds_dict, scopes=SCOPES)
    except Exception as e:
        print(f"❌ Erro na autenticação Google: {e}")
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

    header_nomes = [h.strip() for h in todos_dados[0]] # Cabeçalho na Linha 1
    dados_operacionais = todos_dados[3:] # Dados começam na Linha 4

    hoje = datetime.now(FUSO_HORARIO_SP).date()
    print(f"📅 Hoje: {hoje.strftime('%d/%m/%Y')}")

    linha_ativa = None
    texto_semana = ""
    data_limite_str = ""

    # Localizar a semana correta
    for i, linha in enumerate(dados_operacionais):
        if len(linha) < 9: continue
        
        texto_col_I = linha[8].strip() # Coluna I
        ano_str = linha[3].strip()     # Coluna D
        match = re.search(r'\((\d{2}/\d{2})\s*a\s*(\d{2}/\d{2})\)', texto_col_I)

        if match and ano_str.isdigit():
            try:
                dt_fim = datetime.strptime(f"{match.group(2)}/{ano_str}", "%d/%m/%Y").date()
                dt_ini = datetime.strptime(f"{match.group(1)}/{ano_str}", "%d/%m/%Y").date()
                
                if dt_fim.month < dt_ini.month: # Ajuste virada de ano
                    dt_fim = dt_fim.replace(year=dt_fim.year + 1)

                if dt_ini <= hoje <= dt_fim:
                    linha_ativa = linha
                    texto_semana = texto_col_I
                    data_limite_str = match.group(2)
                    break
            except ValueError: continue

    if not linha_ativa:
        return None, "Nenhuma semana ativa encontrada para hoje."

    pendencias_nomes = []
    ids_para_marcar = []

    # Verificar pendências (Coluna J em diante)
    for i in range(9, len(linha_ativa)):
        if i >= len(header_nomes): break
        
        status = linha_ativa[i].strip().upper()
        nome_coluna = header_nomes[i]
        
        # SÓ ENTRA NA LISTA SE FOR "NÃO REALIZADO"
        # Campos vazios (férias/ausência) são ignorados automaticamente
        if status == "NÃO REALIZADO":
            pendencias_nomes.append(f"❌ {nome_coluna}")
            
            # Busca o ID no dicionário para a menção (@tag)
            user_id = MAPEAMENTO_EQUIPE.get(nome_coluna.upper())
            if user_id:
                ids_para_marcar.append(user_id)

    return {
        "semana": texto_semana,
        "data_limite": data_limite_str,
        "lista_formatada": "\n".join(pendencias_nomes),
        "ids": ids_para_marcar,
        "qtd": len(pendencias_nomes)
    }, None

# --- ENVIO WEBHOOK ---
def enviar_webhook(mensagem, webhook_url, user_ids=None):
    if not webhook_url: return
    payload = {
        "tag": "text",
        "text": { "format": 1, "content": mensagem }
    }
    if user_ids:
        payload["text"]["mentioned_list"] = user_ids

    try:
        requests.post(webhook_url, json=payload).raise_for_status()
        print("✅ Notificação enviada.")
    except Exception as e:
        print(f"❌ Erro Webhook: {e}")

# --- EXECUÇÃO PRINCIPAL ---
def main():
    webhook_url = os.environ.get('WEBHOOK_URL') or os.environ.get('SEATALK_WEBHOOK_URL')
    sheet_id = os.environ.get('SHEET_ID') or os.environ.get('SPREADSHEET_ID')
    creds_var = os.environ.get('GSPREAD_CREDENTIALS') or os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')

    if not all([webhook_url, sheet_id, creds_var]):
        print("⛔ Variáveis de ambiente faltando.")
        return

    cliente = autenticar_google(creds_var)
    if not cliente: return

    resultado, erro = buscar_pendencias_safety_walk(cliente, sheet_id)

    if erro:
        print(f"ℹ️ {erro}")
        return

    if resultado["qtd"] > 0:
        msg = (
            f"⚠️ **Safety Walk Pendente**\n\n"
            f"📅 Período: {resultado['semana']}\n"
            f"❗ {resultado['qtd']} colaboradores pendentes:\n\n"
            f"{resultado['lista_formatada']}\n\n"
            f"Por favor, regularizar até o dia {resultado['data_limite']}!"
        )
        enviar_webhook(msg, webhook_url, user_ids=resultado['ids'])
    else:
        print("✅ Nenhuma pendência encontrada.")

if __name__ == "__main__":
    main()
