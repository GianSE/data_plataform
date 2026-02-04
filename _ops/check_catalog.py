from flows_master.catalog import FLOWS_CATALOG

print("🧐 Verificando integridade do Catálogo...")

ids_vistos = set()

for i, item in enumerate(FLOWS_CATALOG):
    # 1. Verifica chaves obrigatórias
    if "id" not in item or "deploy_path" not in item:
        print(f"❌ Erro no item {i}: Faltando 'id' ou 'deploy_path'")
        exit(1)
    
    # 2. Verifica duplicidade de ID
    if item["id"] in ids_vistos:
        print(f"❌ Erro: ID duplicado encontrado: {item['id']}")
        exit(1)
    ids_vistos.add(item["id"])
    
    # 3. Verifica se a dependência existe
    for dep in item.get("upstream", []):
        if dep not in ids_vistos:
            # Nota: Isso obriga você a declarar o Pai antes do Filho na lista (o que é bom)
            print(f"❌ Erro Lógico: '{item['id']}' depende de '{dep}', mas '{dep}' não foi declarado antes dele.")
            exit(1)

print("✅ Catálogo VÁLIDO! Pode fazer o deploy.")