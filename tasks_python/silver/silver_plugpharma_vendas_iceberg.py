import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyiceberg.catalog import load_catalog
import pyarrow as pa
import os

# -------------------- CONFIGURAÇÕES --------------------
STORAGE_OPTIONS = {
    "endpoint_url": "http://192.168.21.251:9000",
    "aws_access_key_id": "minioadmin",      
    "aws_secret_access_key": "minioadmin",   
    "region_name": "us-east-1",
    "allow_http": "true"
}

# Configuração do Catálogo Iceberg (via Nessie) apontando para o bucket 'iceberg'
catalog = load_catalog(
    "default",
    **{
        "type": "sql",
        "uri": "sqlite:////app/tasks_python/silver/iceberg_catalog.db", # Caminho no seu container
        "warehouse": "s3://iceberg",
        "s3.endpoint": "http://192.168.21.251:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.force-path-style": "true",
        "s3.region": "us-east-1"
    }
)

# 2. GARANTE QUE O NAMESPACE EXISTE (Adicione isso aqui)
try:
    catalog.create_namespace("silver")
    print("✅ Namespace 'silver' verificado/criado.")
except Exception:
    # Se já existir, ele segue o baile
    pass

BUCKET_BRONZE_BASE = "s3://bronze/vendas-plugpharma"
# O nome da tabela será usado para criar a pasta dentro do bucket iceberg
NOME_TABELA_ICEBERG = "silver.ice_vendas_plugpharma"

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
MAPA_MESES_REVERSO = {v: k for k, v in MAPA_MESES.items()}

def processar_mes_iceberg(ano, mes_num):
    mes_str = f"{mes_num:02d}"
    mes_nome = MAPA_MESES_REVERSO.get(mes_num)
    path_origem = f"{BUCKET_BRONZE_BASE}/ano={ano}/mes={mes_str}/*.parquet"
    
    print(f"-> Lendo Bronze: {path_origem}")

    try:
        q = pl.scan_parquet(
            path_origem, 
            storage_options=STORAGE_OPTIONS,
            missing_columns="insert" 
        )

        schema_origem = set(q.collect_schema().names())
        renames_validos = {k: v for k, v in MAPA_RENOMEAR.items() if k in schema_origem}
        q = q.rename(renames_validos)

        q = q.with_columns(
            pl.lit(ano).cast(pl.Int16).alias("ano_venda"),
            pl.lit(mes_nome).alias("mes_venda"),
            pl.lit(mes_num).cast(pl.Int8).alias("mes_num")
        )

        schema_pos_rename = set(q.collect_schema().names())
        
        cols_para_criar = [
            pl.col("codigo_de_barras_produto").cast(pl.Utf8).alias("GTIN") if "codigo_de_barras_produto" in schema_pos_rename else pl.lit(None).cast(pl.Utf8).alias("GTIN"),
            pl.lit(datetime.now()).alias("data_insercao")
        ]
        
        # [SEU BLOCO DE LÓGICA DE LIMPEZA DE CPF/CNPJ]
        
        q = q.with_columns(cols_para_criar)
        
        cols_expr = []
        schema_final_disponivel = set(q.collect_schema().names())
        for nome, tipo in SCHEMA_TARGET.items():
            if nome in schema_final_disponivel:
                cols_expr.append(pl.col(nome).cast(tipo, strict=False))
            else:
                cols_expr.append(pl.lit(None).cast(tipo).alias(nome))
        
        df_local = q.select(cols_expr).collect()
        
        if df_local.height > 0:
            table_arrow = df_local.to_arrow()
            
            try:
                table = catalog.load_table(NOME_TABELA_ICEBERG)
                table.append(table_arrow)
            except Exception:
                # Na criação, ele usará o 'warehouse' configurado acima no catalog
                table = catalog.create_table(
                    NOME_TABELA_ICEBERG,
                    schema=table_arrow.schema
                )
                table.append(table_arrow)
                
            print(f"   ✅ {df_local.height} linhas enviadas para s3://iceberg/{NOME_TABELA_ICEBERG}")
        else:
            print(f"   ⚠️ Pasta sem dados para {ano}-{mes_str}")

    except Exception as e:
        print(f"   ❌ ERRO em {ano}-{mes_str}: {e}")

def main():
    data_cursor = datetime(2025, 1, 1)
    data_final = datetime.now()
    while data_cursor <= data_final:
        processar_mes_iceberg(data_cursor.year, data_cursor.month)
        data_cursor += relativedelta(months=1)

if __name__ == "__main__":
    main()