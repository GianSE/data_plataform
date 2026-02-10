import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os

# -------------------- CONFIGURAÇÕES MINIO --------------------
STORAGE_OPTIONS = {
    "endpoint_url": "http://192.168.21.251:9000",
    "aws_access_key_id": "minioadmin",      
    "aws_secret_access_key": "minioadmin",   
    "region_name": "us-east-1",
    "allow_http": "true"
}

BUCKET_BRONZE = "s3://bronze/vendas-plugpharma/**/*.parquet"
# O Bucket Silver agora é a raiz, as pastas serão criadas dinamicamente
BUCKET_SILVER_ROOT = "s3://silver/silver_plugpharma_vendas/"

# -------------------- MAPAS E SCHEMA --------------------
MAPA_RENOMEAR = {
    "ERP": "erp_loja", "CNPJ.1": "cnpj_loja", "Razão Social.1": "razao_social_loja", 
    "Última Data de Movimentação": "ultima_data_movimentacao_loja",
    "Cargo": "cargo_colaborador", "Nome": "nome_colaborador", "CPF": "cpf_colaborador",
    "Ano": "ano_venda", "Mês / Ano": "mes_ano_venda", "Mês": "mes_venda", 
    "Dia": "dia_venda", "Hora": "hora_venda", 
    "CNPJ.2": "cnpj_fabricante", "Razão Social.2": "razao_social_fabricante", "Nome Fantasia.2": "nome_fantasia_fabricante",
    "Grupo Principal": "grupo_principal_produto", "Tipo de Produto": "tipo_de_produto",
    "Tipo de Lista": "tipo_de_lista_produto", "Descrição": "descricao_produto", 
    "Apresentação": "apresentacao_produto", "Código Interno": "codigo_interno_produto", 
    "Código de Barras": "codigo_de_barras_produto", "PBM": "pbm", "Tipo de Pessoa": "tipo_de_pessoa_cliente",
    "UF.4": "uf_cliente", "Cidade.3": "cidade_cliente", "Sexo": "sexo_cliente", "Idade": "idade_cliente",
    "Nome.1": "nome_cliente", "CPF / CNPJ": "cpf_cnpj_cliente", 
    "Telefone Celular": "telefone_celular_cliente", "Telefone Fixo": "telefone_fixo_cliente", 
    "Endereço": "endereco_cliente", "E-mail": "email_cliente", 
    "Data de Nascimento.1": "data_nascimento_cliente", "CEP": "cep_cliente", 
    "Tipo.1": "tipo_de_nota", "Tipo.2": "tipo_de_operacao", "Tipo.3": "tipo_de_pagamento", 
    "Clientes Atendidos": "qtd_clientes_atendidos", "CMV Unitário": "cmv_unitario", 
    "CMV Total": "cmv_total", "Desconto Total": "desconto_total", 
    "Quantidade": "qtd_de_produtos", "Quantidade Estoque": "qtd_estoque", 
    "Valor Bruto Total": "valor_bruto_total", "Valor Bruto Unitário": "valor_bruto_unitario",
    "Valor Líquido Total": "valor_liquido_total", "Valor Líquido Unitário": "valor_liquido_unitario"
}

SCHEMA_TARGET = {
    'data_venda': pl.Date, # Ajustado para Date conforme seu segundo with_columns
    'erp_loja': pl.Utf8, 'cnpj_loja': pl.Utf8, 'razao_social_loja': pl.Utf8,
    'ultima_data_movimentacao_loja': pl.Date,
    'cargo_colaborador': pl.Utf8, 'nome_colaborador': pl.Utf8, 'cpf_colaborador': pl.Utf8,
    'ano_venda': pl.Int16, 'mes_ano_venda': pl.Utf8, 'mes_venda': pl.Utf8,
    'dia_mes_venda': pl.Utf8, 'dia_venda': pl.Int16, 'hora_venda': pl.Int16,
    'cnpj_fabricante': pl.Utf8, 'razao_social_fabricante': pl.Utf8, 'nome_fantasia_fabricante': pl.Utf8,
    'grupo_principal_produto': pl.Utf8, 'tipo_de_produto': pl.Utf8,
    'tipo_de_lista_produto': pl.Utf8, 'descricao_produto': pl.Utf8, 'apresentacao_produto': pl.Utf8, 
    'codigo_interno_produto': pl.Utf8, 'GTIN': pl.Utf8,
    'codigo_de_barras_normalizado_produto': pl.Utf8,
    'pbm': pl.Utf8, 'tipo_de_pessoa_cliente': pl.Utf8,
    'uf_cliente': pl.Utf8, 'cidade_cliente': pl.Utf8, 'sexo_cliente': pl.Utf8, 'idade_cliente': pl.Int16,
    'nome_cliente': pl.Utf8, 'cpf_cnpj_cliente': pl.Utf8,
    'telefone_celular_cliente': pl.Utf8, 'telefone_fixo_cliente': pl.Utf8, 'endereco_cliente': pl.Utf8,
    'email_cliente': pl.Utf8, 'data_nascimento_cliente': pl.Date, 'cep_cliente': pl.Utf8,
    'tipo_de_nota': pl.Utf8, 'tipo_de_operacao': pl.Utf8, 'tipo_de_pagamento': pl.Utf8,
    'qtd_clientes_atendidos': pl.Int32, 
    'cmv_unitario': pl.Float64, 'cmv_total': pl.Float64,
    'desconto_total': pl.Float64, 'qtd_de_produtos': pl.Int32, 'qtd_estoque': pl.Int32,
    'valor_bruto_total': pl.Float64, 'valor_bruto_unitario': pl.Float64,
    'valor_liquido_total': pl.Float64, 'valor_liquido_unitario': pl.Float64, 
    'data_insercao': pl.Datetime
}

MAPA_MESES = {
    'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Abril': 4, 'Maio': 5, 'Junho': 6,
    'Julho': 7, 'Agosto': 8, 'Setembro': 9, 'Outubro': 10, 'Novembro': 11, 'Dezembro': 12
}

# Mapa reverso para filtrar na Bronze (Número -> Nome)
MAPA_MESES_REVERSO = {v: k for k, v in MAPA_MESES.items()}

def processar_mes(ano, mes_num):
    """Processa um único mês e salva particionado"""
    mes_nome = MAPA_MESES_REVERSO.get(mes_num)
    
    # Define o caminho de destino no formato Hive Partitioning
    # Ex: s3://silver/.../ano_venda=2024/mes_venda=Janeiro/data.parquet
    path_destino = f"{BUCKET_SILVER_ROOT}ano_venda={ano}/mes_venda={mes_nome}/part-001.parquet"
    
    print(f"-> Processando: {ano} - {mes_nome} ...")

    try:
        # 1. SCAN
        q = pl.scan_parquet(BUCKET_BRONZE, storage_options=STORAGE_OPTIONS)

        # 2. RENOMEAR
        q = q.rename(MAPA_RENOMEAR)

        # 3. FILTRAR (Aqui acontece a mágica da RAM)
        # Filtramos a fonte ANTES de qualquer processamento pesado
        q = q.filter(
            (pl.col("ano_venda").cast(pl.Int32, strict=False) == ano) &
            (pl.col("mes_venda") == mes_nome)
        )

        # 4. TRANSFORMAÇÕES
        q = q.with_columns(
            pl.col("codigo_de_barras_produto").cast(pl.Utf8).alias("GTIN"),
            pl.col("mes_venda").map_dict(MAPA_MESES).alias("mes_num")
        )

        q = q.with_columns(
            pl.datetime(
                pl.col("ano_venda").cast(pl.Int32, strict=False),
                pl.col("mes_num").cast(pl.Int8, strict=False),
                pl.col("dia_venda").cast(pl.Int8, strict=False),
                pl.col("hora_venda").cast(pl.Int8, strict=False).fill_null(0)
            ).alias("data_venda_completa"), # Mantive para não perder a info de hora

            pl.date(
                pl.col("ano_venda").cast(pl.Int32, strict=False),
                pl.col("mes_num").cast(pl.Int8, strict=False),
                pl.col("dia_venda").cast(pl.Int8, strict=False),
            ).alias("data_venda"),

            pl.col("cnpj_loja").cast(pl.Utf8).str.replace_all(r"\D", "").str.zfill(14),
            pl.col("cpf_colaborador").cast(pl.Utf8).str.replace_all(r"\D", "").str.zfill(11),
            pl.col("cpf_cnpj_cliente").cast(pl.Utf8).str.replace_all(r"\D", ""),
            pl.col("codigo_de_barras_produto").cast(pl.Utf8).str.replace_all(r"\D", "").str.zfill(14).alias("codigo_de_barras_normalizado_produto"),
            
            pl.col("ultima_data_movimentacao_loja").cast(pl.Utf8).str.to_date(format="%Y-%m-%d", strict=False),
            pl.col("data_nascimento_cliente").cast(pl.Utf8).str.to_date(format="%Y-%m-%d", strict=False),
            
            pl.lit(datetime.now()).alias("data_insercao")
        )

        # 5. SELECT FINAL
        cols_expr = []
        for nome, tipo in SCHEMA_TARGET.items():
            if nome in q.columns:
                cols_expr.append(pl.col(nome).cast(tipo, strict=False))
            else:
                cols_expr.append(pl.lit(None).cast(tipo).alias(nome))
        
        q = q.select(cols_expr)

        # Verifica se tem dados antes de gravar (para não criar arquivo vazio)
        # O collect() aqui traz para RAM, mas como é só 1 mês filtrado, é leve.
        df_local = q.collect()
        
        if df_local.height > 0:
            print(f"   Salvando {df_local.height} linhas em: {path_destino}")
            df_local.write_parquet(
                path_destino,
                storage_options=STORAGE_OPTIONS,
                compression="snappy",
                row_group_size=500_000
            )
        else:
            print(f"   Sem dados para {ano}-{mes_nome}. Pulando.")

    except Exception as e:
        print(f"   ERRO ao processar {ano}-{mes_nome}: {e}")

def main_loop():
    # Define data de início (3 anos atrás a partir de hoje)
    data_atual = datetime.now()
    data_cursor = data_atual - relativedelta(years=3)
    
    # Loop mês a mês até hoje
    while data_cursor <= data_atual:
        ano = data_cursor.year
        mes = data_cursor.month
        
        processar_mes(ano, mes)
        
        # Avança 1 mês
        data_cursor += relativedelta(months=1)

    print("--- Processamento Mês a Mês Concluído ---")

if __name__ == "__main__":
    main_loop()