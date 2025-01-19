import os


def clear_null_files(directory: str):
    """
    Exclui arquivos cujo conteúdo é 'null' do diretório especificado.

    Args:
        directory (str): O caminho do diretório onde os arquivos serão verificados e excluídos se o conteúdo for 'null'.
    """
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            with open(file_path, "r") as file:
                content = file.read().strip()
                if content == "null":
                    os.remove(file_path)
                    print(f"Arquivo com conteúdo 'null' excluído: {file_path}")


if __name__ == "__main__":
    directory = "data/json_data/raw_club_stats"
    clear_null_files(directory)
