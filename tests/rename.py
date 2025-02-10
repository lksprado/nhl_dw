import os

# Defina o diretório onde os arquivos estão localizados
directory = "/media/lucas/Files/2.Projetos/nhl-dw/data/csv_data/raw/raw_game_details"

# Percorrer todos os arquivos no diretório
for filename in os.listdir(directory):
    if filename.startswith("raw_"):  # Verifica se começa com "raw_"
        new_filename = filename[4:]  # Remove os 4 primeiros caracteres ("raw_")
        old_path = os.path.join(directory, filename)
        new_path = os.path.join(directory, new_filename)

        os.rename(old_path, new_path)  # Renomeia o arquivo
        print(f"Renomeado: {filename} → {new_filename}")
