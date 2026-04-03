import boto3
import json
import pandas as pd
from datetime import datetime
import requests

# CONFIGURAÇÃO
ACCESS_KEY = "SUA_CHAVE_AWS_AQUI"
SECRET_KEY = "SEU_SECRET_AWS_AQUI"
API_KEY_FUTEBOL = "SUA_API_KEY_FOOTBALL_AQUI"

# 1. DEFINIR A CHAVE AQUI NO TOPO (COM ASPAS!)
API_KEY_FUTEBOL = "COLOQUE_SUA_CHAVE_AQUI"

# Inicializa o cliente S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

def extrair_dados_reais(api_key):
    """Busca os jogos do Palmeiras com lógica de resultado corrigida"""
    print("⚽ Conectando à API-Football e processando resultados...")
    
    url = "https://v3.football.api-sports.io/fixtures"
    querystring = {"team": "121", "league": "71", "season": "2024"} 
    
    headers = {
        'x-rapidapi-host': "v3.football.api-sports.io",
        'x-rapidapi-key': api_key
    }

    response = requests.get(url, headers=headers, params=querystring)
    res = response.json()

    jogos_reais = []
    for item in res.get('response', []):
        # 1. Identifica os gols
        gols_home = item['goals']['home'] if item['goals']['home'] is not None else 0
        gols_away = item['goals']['away'] if item['goals']['away'] is not None else 0
        
        # 2. Identifica se o Palmeiras era Mandante ou Visitante
        sou_mandante = item['teams']['home']['name'] == 'Palmeiras'
        
        # 3. Lógica de Resultado (O "Pulo do Gato")
        if gols_home == gols_away:
            resultado = "Empate"
        elif sou_mandante:
            resultado = "Vitoria" if gols_home > gols_away else "Derrota"
        else:
            resultado = "Vitoria" if gols_away > gols_home else "Derrota"
            
        jogo = {
            "data": item['fixture']['date'][:10],
            "adversario": item['teams']['away']['name'] if sou_mandante else item['teams']['home']['name'],
            "placar": f"{gols_home}-{gols_away}",
            "resultado": resultado,
            "equipe": "Palmeiras"
        }
        jogos_reais.append(jogo)
        
    return jogos_reais

def carregar_para_s3(dados, camada):
    """Envia os dados formatados para o Athena (JSON Lines)"""
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    nome_arquivo = f"{camada}/dados_palmeiras_{data_hoje}.json"
    
    conteudo_final = "\n".join([json.dumps(jogo) for jogo in dados])
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=nome_arquivo,
        Body=conteudo_final
    )
    print(f"✅ SUCESSO: Arquivo enviado para a camada '{camada}'!")

# EXECUÇÃO DO PIPELINE

if __name__ == "__main__":
    # Agora a variável API_KEY_FUTEBOL existe e o Python vai reconhecer!
    
    dados_brutos = extrair_dados_reais(API_KEY_FUTEBOL)
    
    if dados_brutos:
        # Carga Bronze
        carregar_para_s3(dados_brutos, "bronze")
        
        # Transformação
        df = pd.DataFrame(dados_brutos)
        df['equipe'] = 'Palmeiras' 
        dados_limpos = df.to_dict(orient='records')
        
        # Carga Silver
        carregar_para_s3(dados_limpos, "silver")
        print("\n🚀 GOL! Pipeline finalizado com dados REAIS!")
    else:
        print("⚠️ Nenhum dado encontrado para processar.")