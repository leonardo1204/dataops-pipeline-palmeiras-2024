import boto3
import json
import pandas as pd
from datetime import datetime
import requests

# --- CONFIGURAÇÃO (Preencha aqui!) ---
ACCESS_KEY = "SUA_CHAVE_AWS_AQUI"
SECRET_KEY = "SEU_SECRET_AWS_AQUI"
API_KEY_FUTEBOL = "SUA_API_KEY_FUTEBOL_AQUI"
BUCKET_NAME = "NOME_DO_BUCKET" 
# -------------------------------------

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
    # Filtro exato para o Plano Free (Brasileirão 2024)
    querystring = {"team": "121", "league": "71", "season": "2024"}
    
    headers = {
        'x-rapidapi-host': "v3.football.api-sports.io",
        'x-rapidapi-key': api_key
    }

    try:
        response = requests.get(url, headers=headers, params=querystring)
        res = response.json()

        # LOG DE DEPURAÇÃO: Se houver erro na API, ele avisa aqui
        if res.get('errors'):
            print(f"❌ Erro da API: {res['errors']}")
            return []

        jogos_reais = []
        for item in res.get('response', []):
            gols_home = item['goals']['home'] if item['goals']['home'] is not None else 0
            gols_away = item['goals']['away'] if item['goals']['away'] is not None else 0
            
            sou_mandante = item['teams']['home']['name'] == 'Palmeiras'
            
            # Lógica de Resultado Refatorada
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
    except Exception as e:
        print(f"❌ Erro na requisição: {e}")
        return []

def carregar_para_s3(dados, camada):
    """Envia os dados formatados para o Athena (JSON Lines)"""
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    # Organiza em pastas bronze/ e silver/ dentro do S3
    nome_arquivo = f"{camada}/dados_palmeiras_{data_hoje}.json"
    
    # Converte a lista para o formato NDJSON (importante para o Athena!)
    conteudo_final = "\n".join([json.dumps(jogo) for jogo in dados])
    
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=nome_arquivo,
            Body=conteudo_final
        )
        print(f"✅ SUCESSO: Arquivo enviado para a camada '{camada}'!")
    except Exception as e:
        print(f"❌ Erro no upload S3: {e}")

# --- EXECUÇÃO DO PIPELINE ---

if __name__ == "__main__":
    # Inicia a extração
    dados_brutos = extrair_dados_reais(API_KEY_FUTEBOL)
    
    if dados_brutos and len(dados_brutos) > 0:
        print(f"📊 {len(dados_brutos)} jogos encontrados!")
        
        # Carga Bronze (Raw)
        carregar_para_s3(dados_brutos, "bronze")
        
        # Transformação Simples com Pandas
        df = pd.DataFrame(dados_brutos)
        # Garantimos que a coluna equipe está preenchida
        df['equipe'] = 'Palmeiras' 
        dados_limpos = df.to_dict(orient='records')
        
        # Carga Silver (Curated)
        carregar_para_s3(dados_limpos, "silver")
        
        print("\n🚀 GOL! Pipeline finalizado com dados REAIS e corrigidos!")
    else:
        print("\n⚠️ Nenhum dado encontrado. Verifique sua API Key ou o limite do plano.")