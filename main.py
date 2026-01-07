import pandas as pd
import gspread
import requests
import base64
import re
from datetime import datetime
from pytz import timezone
import os
import json

# ==============================================================================
# ⚙️ CONFIGURAÇÃO DE LAYOUT (LIMITADO DAS LINHAS 6 A 57)
# ==============================================================================

# 1. LIMITES DE LINHAS (Python começa do 0. Linha 6 Excel = Índice 5)
INDICE_LINHA_CABECALHO = 0       # Linha 1: Onde estão os nomes (Alvaro, Wellington...)
INDICE_LINHA_INICIO = 5          # Linha 6: Início da busca
INDICE_LINHA_FIM = 57            # Linha 57: Fim da busca (O Python exclui o último número, então vai até 57)

# 2. COLUNAS (A=0, B=1 ... J=9 ... N=13)
INDICE_COLUNA_PILAR = 1          # Coluna B: "Safety Walk"
INDICE_COLUNA_DATA = 9           # Coluna J: "Sem 02 (05/01 a 10/01)"
INDICE_INICIO_COLUNAS_NOMES = 13 # Coluna N: Primeiro nome (Alvaro)

# 3. FILTRO
PILAR_ALVO = "Safety Walk"       # Texto para confirmar se é a linha certa

# 4. GERAIS
NOME_ABA = 'Reporte'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
FUSO_HORARIO_SP = timezone('America/Sao_Paulo')
# ==============================================================================

# 👥 CADASTRO DA EQUIPE (Preencha os IDs que faltam)
MAPEAMENTO_EQUIPE = {
    "ALVARO GOMEZ RUEDA": "",
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
    "ANDERSON ZANUTO": "1269362711",
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
    "EDILENE AUGUSTO": "1185463777",
}

def autenticar_google(creds_var):
    try:
        try:
            creds_dict = json.loads(creds_var)
        except json.JSONDecodeError:
            decoded_bytes = base64.b64decode(creds_var, validate=True)
            creds_dict = json.loads(decoded_bytes.decode("utf-8"))
        return gspread.service_account_from_dict(creds_dict, scopes=SCOPES)
    except Exception as e:
        print(f"❌ Erro na autenticação Google: {e}")
        return None

def buscar_pendencias_safety_walk(cliente, spreadsheet_id):
    if not cliente: return None, "Cliente não conectado."
    try:
        sheet = cliente.open_by_key(spreadsheet_id)
        aba = sheet.worksheet(NOME_ABA)
        todos_dados = aba.get_all_values()
    except Exception as e:
        return None, f"Erro ao abrir aba '{NOME_ABA}': {e}"

    if not todos_dados: return None, "Aba vazia."

    try:
        header_nomes = [h.strip() for h in todos_dados[INDICE_LINHA_CABECALHO]]
        
        # --- CORTE EXATO: LINHAS 6 A 57 ---
        # O Python usa fatiamento [inicio : fim], onde o fim não é incluído.
        # Linha 6 = índice 5. Linha 57 = índice 56. Para incluir a 57, usamos 57 no slice.
        dados_operacionais = todos_dados[INDICE_LINHA_INICIO : INDICE_LINHA_FIM]
        
    except IndexError:
        return None, "Erro de Índice: A planilha tem menos linhas do que o configurado."

    hoje = datetime.now(FUSO_HORARIO_SP).date()
    # Debug: Para testar, descomente e mude a data:
    # hoje = datetime(2025, 1, 7).date() 
    
    print(f"📅 Data Base: {hoje.strftime('%d/%m/%Y')}")

    linha_ativa = None
    texto_semana = ""
    data_limite_str = ""

    # --- VARREDURA NO INTERVALO ESPECÍFICO ---
    for linha in dados_operacionais:
        if len(linha) <= INDICE_INICIO_COLUNAS_NOMES: continue
        
        # 1. Checa Pilar (Coluna B)
        pilar_atual = linha[INDICE_COLUNA_PILAR].strip()
        if PILAR_ALVO.lower() not in pilar_atual.lower():
            continue 

        # 2. Checa Data (Coluna J)
        texto_data = linha[INDICE_COLUNA_DATA].strip()
        datas_encontradas = re.findall(r'(\d{1,2}/\d{1,2})', texto_data)
        
        if len(datas_encontradas) >= 2:
            try:
                ano_atual = hoje.year
                dt_ini = datetime.strptime(f"{datas_encontradas[0]}/{ano_atual}", "%d/%m/%Y").date()
                dt_fim = datetime.strptime(f"{datas_encontradas[1]}/{ano_atual}", "%d/%m/%Y").date()
                
                # Ajuste de virada de ano
                if dt_fim.month < dt_ini.month:
                    dt_fim = dt_fim.replace(year=ano_atual + 1)
                
                # Validação
                if dt_ini <= hoje <= dt_fim:
                    print(f"✅ Encontrado na linha: {pilar_atual} | {texto_data}")
                    linha_ativa = linha
                    texto_semana = texto_data
                    data_limite_str = datas_encontradas[1]
                    break
            except ValueError:
                continue

    if not linha_ativa:
        return None, "Nenhuma linha de 'Safety Walk' encontrada para hoje neste intervalo."

    pendencias_nomes = []
    ids_para_marcar = []

    # --- VERIFICAÇÃO DE PENDÊNCIAS ---
    for i in range(INDICE_INICIO_COLUNAS_NOMES, len(linha_ativa)):
        if i >= len(header_nomes): break
        
        status = linha_ativa[i].strip().upper()
        nome_lider = header_nomes[i].strip()
        
        if not nome_lider: continue

        if status == "NÃO REALIZADO":
            pendencias_nomes.append(f"❌ {nome_lider}")
            user_id = MAPEAMENTO_EQUIPE.get(nome_lider.upper())
            if user_id and user_id not in ids_para_marcar:
                ids_para_marcar.append(user_id)

    return {
        "semana": texto_semana,
        "data_limite": data_limite_str,
        "lista_formatada": "\n".join(pendencias_nomes),
        "ids": ids_para_marcar,
        "qtd": len(pendencias_nomes)
    }, None

def enviar_webhook(mensagem, webhook_url, user_ids=None):
    if not webhook_url: return
    payload = {
        "tag": "text",
        "text": { "format": 1, "content": mensagem }
    }
    if user_ids: payload["text"]["mentioned_list"] = user_ids
    
    try:
        requests.post(webhook_url, json=payload).raise_for_status()
        print(f"✅ Notificação enviada para {len(user_ids) if user_ids else 0} pessoas.")
    except Exception as e:
        print(f"❌ Erro Webhook: {e}")

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
            f"⚠️ **Safety Walk Pendente** ⚠️\n\n"
            f"📅 Período: {resultado['semana']}\n"
            f"❗ {resultado['qtd']} colaboradores pendentes:\n\n"
            f"{resultado['lista_formatada']}\n\n"
            f"Por favor, regularizar até o dia {resultado['data_limite']}!"
        )
        enviar_webhook(msg, webhook_url, user_ids=resultado['ids'])
    else:
        print("✅ Tudo certo! Nenhuma pendência encontrada.")

if __name__ == "__main__":
    main()
