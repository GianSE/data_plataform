# catalog.py

FLOWS_CATALOG = [
  
    # 1. BRONZE (A Base)
    {
        "id": "bronze_acode",  # Apelido interno (usado para referência)
        "deploy_path": "Bronze Acode Compras/Pipeline Bronze Acode Compras", # Nome Real no Prefect
        "upstream": []  # Lista vazia = Não depende de ninguém, roda assim que o Master começar
    },

    # 2. SILVER (depende da bronze)
    {
        "id": "comercial_acode",
        "deploy_path": "Pipeline Acode Compras Comercial/Pipeline Acode Compras Comercial",
        "upstream": ["bronze_acode"]
    }
]