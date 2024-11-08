from neo4j import GraphDatabase
import csv
import re
import ast

# Configurações de conexão com o Neo4j
uri = "neo4j+s://5379d090.databases.neo4j.io"  # Altere para o endereço do seu Neo4j se necessário
username = "neo4j"
password = "ntSSsgw8XfO1a2D5rkSd8k0Hn8WbtMtS5x2i0IxuT5Y"  # Insira sua senha aqui

# Inicializando o driver do Neo4j
driver = GraphDatabase.driver(uri, auth=(username, password))

# Função para inserir um Pokémon no banco de dados
def inserir_pokemon(tx, pokemon):
    # Extraindo apenas o valor numérico do peso usando uma expressão regular
    peso_str = pokemon['pokemon_peso']
    peso_match = re.search(r"(\d+(\.\d+)?)", peso_str)  # Captura o número decimal antes de qualquer unidade
    peso = float(peso_match.group(0)) if peso_match else None  # Converte para float ou None se não encontrar

    # Insere Pokémon, tipos, habilidades e evoluções
    tx.run("""
        MERGE (p:Pokemon {id: $pokemon_id})
        SET p.name = $pokemon_name,
            p.altura = $pokemon_altura,
            p.peso = $pokemon_peso,
            p.url_pagina = $url_pagina
        WITH p
        UNWIND $pokemon_tipos AS tipo
        MERGE (t:Tipo {name: tipo})
        MERGE (p)-[:TEM_TIPO]->(t)
        WITH p
        UNWIND $pokemon_habilidades AS habilidade
        MERGE (h:Habilidade {name: habilidade.nome, url: habilidade.url})
        MERGE (p)-[:TEM_HABILIDADE]->(h)
        WITH p
        UNWIND $pokemon_proximas_evolucoes AS evolucao
        MERGE (e:Pokemon {id: evolucao.numero, name: evolucao.nome, url: evolucao.url})
        MERGE (p)-[:EVOLUI_PARA]->(e)
        """, 
        pokemon_id=pokemon['pokemon_id'],
        pokemon_name=pokemon['pokemon_name'],
        pokemon_altura=pokemon['pokemon_altura'],
        pokemon_peso=peso,
        url_pagina=pokemon['url_pagina'],
        pokemon_tipos=pokemon['pokemon_tipos'],
        pokemon_habilidades=pokemon['pokemon_habilidades'],
        pokemon_proximas_evolucoes=pokemon['pokemon_proximas_evolucoes']
    )

# Função para processar listas de strings no CSV
def processar_lista(campo):
    # Remove espaços e divide em uma lista, assumindo que o delimitador é vírgula
    return [item.strip() for item in campo.split(',') if item.strip()]

# Função para converter uma string de lista de dicionários em uma lista de dicionários
def processar_lista_dicionarios(campo):
    try:
        # Usa ast.literal_eval para converter string para lista de dicionários
        return ast.literal_eval(campo)
    except (SyntaxError, ValueError):
        return []  # Retorna uma lista vazia em caso de erro

# Inserindo todos os dados no Neo4j
def inserir_dados_neo4j(pokemons):
    with driver.session() as session:
        for pokemon in pokemons:
            session.execute_write(inserir_pokemon, pokemon)


# Lendo o arquivo CSV e inserindo os dados no Neo4j
with open('file.csv', 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    pokemons = []

    for row in reader:
        # Processa o campo de tipos e converte para uma lista de strings
        row['pokemon_tipos'] = processar_lista(row['pokemon_tipos'])

        # Converte as habilidades e evoluções para listas de dicionários usando ast.literal_eval
        row['pokemon_habilidades'] = processar_lista_dicionarios(row['pokemon_habilidades'])
        row['pokemon_proximas_evolucoes'] = processar_lista_dicionarios(row['pokemon_proximas_evolucoes'])

        pokemons.append(row)

    # Insere os dados no Neo4j
    inserir_dados_neo4j(pokemons)

# Fechando o driver após a inserção
driver.close()
