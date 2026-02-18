import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os

""" 
1 - "incremental"   (reescreve o mes atual e o 1 mes anterior) 
2 - "range"         (reescreve todos os parquet da data_inio a data_fim)
3 - "pontual"       (reescreve um parquet em específico para tratar erros de envio)
"""

MODO = "incremental"  # incremental | pontual | range

# usado no modo pontual
ANO_PONTUAL = 2021
MES_PONTUAL = 9

# usado no modo range
DATA_INICIO = datetime(2014, 1, 1)
DATA_FIM = datetime(2026, 12, 1)



# -------------------- CONFIGURAÇÕES --------------------
STORAGE_OPTIONS = {
    "endpoint_url": "http://192.168.21.251:9000",
    "aws_access_key_id": "minioadmin",      
    "aws_secret_access_key": "minioadmin",   
    "region_name": "us-east-1",
    "allow_http": "true"
}

BUCKET_BRONZE_BASE = "s3://bronze/vendas-plugpharma" 
BUCKET_SILVER_ROOT = "s3://silver/silver_plugpharma_vendas/"

# (Mantive seu MAPA_RENOMEAR e SCHEMA_TARGET idênticos)
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
    'data_venda': pl.Date,
    'data_venda_completa': pl.Datetime, 
    'erp_loja': pl.Utf8, 'cnpj_loja': pl.Utf8, 'razao_social_loja': pl.Utf8,
    'ultima_data_movimentacao_loja': pl.Date,
    'cargo_colaborador': pl.Utf8, 'nome_colaborador': pl.Utf8, 'cpf_colaborador': pl.Utf8,
    'ano_venda': pl.Int64, 'mes_ano_venda': pl.Utf8, 'mes_venda': pl.Utf8,
    'dia_mes_venda': pl.Utf8, 'dia_venda': pl.Int64, 'hora_venda': pl.Int64,
    'cnpj_fabricante': pl.Utf8, 'razao_social_fabricante': pl.Utf8, 'nome_fantasia_fabricante': pl.Utf8,
    'grupo_principal_produto': pl.Utf8, 'tipo_de_produto': pl.Utf8,
    'tipo_de_lista_produto': pl.Utf8, 'descricao_produto': pl.Utf8, 'apresentacao_produto': pl.Utf8, 
    'codigo_interno_produto': pl.Utf8, 'GTIN': pl.Utf8,
    'codigo_de_barras_normalizado_produto': pl.Utf8,
    'pbm': pl.Utf8, 'tipo_de_pessoa_cliente': pl.Utf8,
    'uf_cliente': pl.Utf8, 'cidade_cliente': pl.Utf8, 'sexo_cliente': pl.Utf8, 'idade_cliente': pl.Int64,
    'nome_cliente': pl.Utf8, 'cpf_cnpj_cliente': pl.Utf8,
    'telefone_celular_cliente': pl.Utf8, 'telefone_fixo_cliente': pl.Utf8, 'endereco_cliente': pl.Utf8,
    'email_cliente': pl.Utf8, 'data_nascimento_cliente': pl.Date, 'cep_cliente': pl.Utf8,
    'tipo_de_nota': pl.Utf8, 'tipo_de_operacao': pl.Utf8, 'tipo_de_pagamento': pl.Utf8,
    'qtd_clientes_atendidos': pl.Int64, 
    'cmv_unitario': pl.Float64, 'cmv_total': pl.Float64,
    'desconto_total': pl.Float64, 'qtd_de_produtos': pl.Int64, 'qtd_estoque': pl.Int64,
    'valor_bruto_total': pl.Float64, 'valor_bruto_unitario': pl.Float64,
    'valor_liquido_total': pl.Float64, 'valor_liquido_unitario': pl.Float64, 
    'data_insercao': pl.Datetime
}

MAPA_MESES = {
    'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Abril': 4, 'Maio': 5, 'Junho': 6,
    'Julho': 7, 'Agosto': 8, 'Setembro': 9, 'Outubro': 10, 'Novembro': 11, 'Dezembro': 12
}
MAPA_MESES_REVERSO = {v: k for k, v in MAPA_MESES.items()}

def processar_mes_direto(ano, mes_num):
    mes_str = f"{mes_num:02d}"
    path_origem = f"{BUCKET_BRONZE_BASE}/ano={ano}/mes={mes_str}/*.parquet"
    mes_nome = MAPA_MESES_REVERSO.get(mes_num)
    path_destino = f"{BUCKET_SILVER_ROOT}ano_venda={ano}/mes_venda={mes_str}/part-001.parquet"
    
    print(f"-> Acessando direto: {path_origem}")

    try:
        # CORREÇÃO DEFINITIVA: missing_columns="insert" GARANTE A UNIÃO DOS SCHEMAS
        q = pl.scan_parquet(
            path_origem, 
            storage_options=STORAGE_OPTIONS,
            missing_columns="insert" 
        )

        schema_origem = set(q.collect_schema().names())
        renames_validos = {k: v for k, v in MAPA_RENOMEAR.items() if k in schema_origem}
        q = q.rename(renames_validos)

        schema_atual = schema_origem.copy()
        for k, v in renames_validos.items():
            schema_atual.remove(k)
            schema_atual.add(v)
        
        schema_atual.update(["ano_venda", "mes_venda", "mes_num"])

        q = q.with_columns(
            pl.lit(ano).cast(pl.Int64).alias("ano_venda"),
            pl.lit(mes_nome).alias("mes_venda"),
            pl.lit(mes_num).cast(pl.Int64).alias("mes_num")
        )

        if "codigo_de_barras_produto" in schema_atual:
             gtin_expr = pl.col("codigo_de_barras_produto").cast(pl.Utf8).alias("GTIN")
        else:
             gtin_expr = pl.lit(None).cast(pl.Utf8).alias("GTIN")

        cols_para_criar = [
            gtin_expr,
            pl.datetime(
                pl.col("ano_venda"),
                pl.col("mes_num"),
                pl.col("dia_venda").cast(pl.Int64, strict=False) if "dia_venda" in schema_atual else pl.lit(1),
                pl.col("hora_venda").cast(pl.Int64, strict=False).fill_null(0) if "hora_venda" in schema_atual else pl.lit(0)
            ).alias("data_venda_completa"),
            pl.date(
                pl.col("ano_venda"),
                pl.col("mes_num"),
                pl.col("dia_venda").cast(pl.Int64, strict=False) if "dia_venda" in schema_atual else pl.lit(1),
            ).alias("data_venda"),
            pl.lit(datetime.now()).alias("data_insercao")
        ]
        
        if "cnpj_loja" in schema_atual:
            cols_para_criar.append(pl.col("cnpj_loja").cast(pl.Utf8).str.replace_all(r"\D", "").str.zfill(14))
        if "cpf_colaborador" in schema_atual:
             cols_para_criar.append(pl.col("cpf_colaborador").cast(pl.Utf8).str.replace_all(r"\D", "").str.zfill(11))
        if "cpf_cnpj_cliente" in schema_atual:
            cols_para_criar.append(pl.col("cpf_cnpj_cliente").cast(pl.Utf8).str.replace_all(r"\D", ""))
        if "codigo_de_barras_produto" in schema_atual:
            cols_para_criar.append(pl.col("codigo_de_barras_produto").cast(pl.Utf8).str.replace_all(r"\D", "").str.zfill(14).alias("codigo_de_barras_normalizado_produto"))
        if "ultima_data_movimentacao_loja" in schema_atual:
            cols_para_criar.append(pl.col("ultima_data_movimentacao_loja").cast(pl.Utf8).str.to_date(format="%Y-%m-%d", strict=False))
        if "data_nascimento_cliente" in schema_atual:
            cols_para_criar.append(pl.col("data_nascimento_cliente").cast(pl.Utf8).str.to_date(format="%Y-%m-%d", strict=False))

        q = q.with_columns(cols_para_criar)
        
        cols_expr = []
        schema_final_disponivel = set(q.collect_schema().names())
        for nome, tipo in SCHEMA_TARGET.items():
            if nome in schema_final_disponivel:
                cols_expr.append(pl.col(nome).cast(tipo, strict=False))
            else:
                cols_expr.append(pl.lit(None).cast(tipo).alias(nome))
        
        q = q.select(cols_expr)
        df_local = q.collect()
        
        if df_local.height > 0:
            print(f"   Salvando {df_local.height} linhas em: {path_destino}")
            df_local.write_parquet(
                path_destino, 
                storage_options=STORAGE_OPTIONS, 
                compression="zstd",        # <--- Mudou de "snappy" para "zstd"
                compression_level=3,       # <--- (Opcional) Define o nível. 3 é o padrão equilibrado.
                row_group_size=100_000     # <--- Sugestão: Alinhe com o DuckDB (era 500k, 100k é melhor para leitura)
            )
        else:
            print(f"   Aviso: Pasta sem dados para {ano}-{mes_str}.")

    except Exception as e:
        msg_erro = str(e)
        if "expanded paths were empty" in msg_erro or "No such file" in msg_erro:
             print(f"   Aviso: Pasta vazia ou sem dados.")
        else:
             print(f"   ERRO CRÍTICO em {ano}-{mes_str}: {msg_erro}")

def main():
    data_atual = datetime.now()

    if MODO == "incremental":
        print("Modo incremental (Mês atual, anterior e anterior-anterior)")
        
        # Mês atual
        processar_mes_direto(data_atual.year, data_atual.month)
        
        # Mês anterior
        mes_anterior = data_atual - relativedelta(months=1)
        processar_mes_direto(mes_anterior.year, mes_anterior.month)

        # Mês anterior ao anterior
        # mes_anterior_2 = data_atual - relativedelta(months=2)
        # processar_mes_direto(mes_anterior_2.year, mes_anterior_2.month)

    elif MODO == "pontual":
        print(f"Modo PONTUAL -> {ANO_PONTUAL}-{MES_PONTUAL:02d}")
        processar_mes_direto(ANO_PONTUAL, MES_PONTUAL)

    elif MODO == "range":
        print(f"Modo RANGE -> {DATA_INICIO.strftime('%Y-%m')} até {DATA_FIM.strftime('%Y-%m')}")

        if DATA_INICIO > DATA_FIM:
            raise ValueError("DATA_INICIO maior que DATA_FIM")

        data_cursor = DATA_INICIO

        while data_cursor <= DATA_FIM:
            processar_mes_direto(data_cursor.year, data_cursor.month)
            data_cursor += relativedelta(months=1)

    else:
        raise ValueError("Modo inválido")

if __name__ == "__main__":
    main()