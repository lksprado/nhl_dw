import unittest
from unittest.mock import MagicMock, Mock, mock_open, patch

from app.extraction.generic_get_results import make_request, save_json
from app.extraction.get_all import *

"""
Resumo:
O teste apresentado verifica o comportamento da função make_request, que faz uma chamada à API usando a biblioteca requests.
Ele usa a biblioteca unittest e a funcionalidade patch para simular o comportamento da chamada HTTP. 
Ele verifica se:
    A função make_request chama requests.get com os argumentos corretos (o URL e o timeout).
    A função make_request retorna os dados esperados (neste caso, o dicionário response_dict).

@patch serve para simular partes específicas dentro de um método. Como funções utilizadas dentro do método

Explicações passo a passo:
1. O teste unitário testa a função make_request() do módulo generic_get_results.py
2. A classe base TestCase serva para criar novos casos de tests. Cada método dentro de uma classe que herda TestCase
    é considerado um teste unitário
3. O decorador @patch é usado para substituir a função requests.get() dentro do método original por um objeto Mock. Isso é feito para que a função
    make_request() não faça uma chamada real para a API, mas sim para um objeto simulado.
4. O parâmetro mock_get é passado para a função de teste. Esse objeto Mock é usado para simular a resposta da API.
5. A response também é mockada com mock_response = Mock() para maior flexibilidade.
6. Criamos uma resposta mockada com o dicionário response_dict = {"team_id": "EDM", "season_id": "20242025"} para imitar Jsin.
7. Artificialmente dizemos que a resposta da API é response_dict com mock_response.json.return_value = response_dict
8. Definimos que o retorno de de requests.get() é mock_response que tem response_dict como resposta.
9. Instanciamos o método original make_request() com a URL da API.
10. Verificamos se a função requests.get() foi chamada com a URL correta e o timeout correto.
11. Verificamos se a resposta da API é igual a response_dict (Um objeto dicionário/Json)
"""


class TestApiCall(unittest.TestCase):
    @patch("requests.get")
    def test_make_request(self, mock_get):
        mock_response = Mock()
        response_dict = {"team_id": "EDM", "season_id": "20242025"}
        mock_response.json.return_value = response_dict
        mock_get.return_value = mock_response

        teams_stats, _ = make_request(
            url="https://api.nhle.com/stats/rest/en/team/summary?seasonId=20242025&TypeId=2"
        )

        mock_get.assert_called_once_with(
            "https://api.nhle.com/stats/rest/en/team/summary?seasonId=20242025&TypeId=2",
            timeout=5,
        )
        self.assertEqual(teams_stats, response_dict)

    #######################################################################################################################

    @patch("os.path.join", return_value="mocked_path/raw_test_file.json")
    @patch("builtins.open", new_callable=mock_open)
    @patch("json.dump")
    def test_save_json(self, mock_json_dump, mock_open_file, mock_path_join):
        # Mock input data
        file_name = "test_file"
        data = {"key": "value"}
        output_json_dir = "mocked_path"

        # Call the function
        save_json(file_name, data, output_json_dir)

        # Verifica se os métodos foram chamados corretamente
        mock_path_join.assert_called_once_with(output_json_dir, "raw_test_file.json")
        mock_open_file.assert_called_once_with(
            "mocked_path/raw_test_file.json", "w", encoding="utf-8"
        )
        mock_json_dump.assert_called_once_with(
            data, mock_open_file(), indent=4, ensure_ascii=False
        )

    #######################################################################################################################

    @patch("app.extraction.get_all.make_request")
    @patch("app.extraction.get_all.save_json")
    def test_get_teams(self, mock_save_json, mock_make_request):
        mock_data = {"teams": [{"team_id": "EDM", "team_name": "Edmonton Oilers"}]}
        mock_make_request.return_value = (mock_data, None)

        get_teams()

        mock_make_request.assert_called_once_with(
            "https://api.nhle.com/stats/rest/en/team"
        )
        mock_save_json.assert_called_once_with(
            "teams", mock_data, "data/json_data/single"
        )

    #######################################################################################################################

    @patch("app.extraction.get_all.make_request")
    @patch("app.extraction.get_all.save_json")
    def test_get_all_games_info(self, mock_save_json, mock_make_request):
        mock_data = {"teams": [{"team_id": "EDM", "team_name": "Edmonton Oilers"}]}
        mock_make_request.return_value = (mock_data, None)

        get_all_games_info()

        mock_make_request.assert_called_with(
            "https://api.nhle.com/stats/rest/en/season"
        )
        mock_save_json.assert_called_with(
            "all_games_info", mock_data, "data/json_data/single"
        )

    #######################################################################################################################

    @patch("app.extraction.get_all.make_request")
    @patch("app.extraction.get_all.save_json")
    def test_get_season_ids(self, mock_save_json, mock_make_request):
        mock_data = {"teams": [{"team_id": "EDM", "team_name": "Edmonton Oilers"}]}
        mock_make_request.return_value = (mock_data, None)

        get_season_ids()

        mock_make_request.assert_called_once_with("https://api-web.nhle.com/v1/season")
        mock_save_json.assert_called_once_with(
            "season_ids", mock_data, "data/json_data/single"
        )

    #######################################################################################################################

    @patch("app.extraction.get_all.make_request")
    @patch("app.extraction.get_all.save_json")
    def test_get_season_info(self, mock_save_json, mock_make_request):
        mock_data = {"teams": [{"team_id": "EDM", "team_name": "Edmonton Oilers"}]}
        mock_make_request.return_value = (mock_data, None)

        get_season_info()

        mock_make_request.assert_called_once_with(
            "https://api.nhle.com/stats/rest/en/season"
        )
        mock_save_json.assert_called_once_with(
            "season_info", mock_data, "data/json_data/single"
        )

    #######################################################################################################################

    @patch("app.extraction.get_all.make_request")
    @patch("app.extraction.get_all.save_json")
    def test_get_standings_now(self, mock_save_json, mock_make_request):
        mock_data = {"teams": [{"team_id": "EDM", "team_name": "Edmonton Oilers"}]}
        mock_make_request.return_value = (mock_data, None)

        get_standings_now()

        mock_make_request.assert_called_once_with(
            "https://api-web.nhle.com/v1/standings/now"
        )
        mock_save_json.assert_called_once_with(
            "standings_now", mock_data, "data/json_data/single"
        )

    #######################################################################################################################

    @patch("app.extraction.get_all.pd.read_csv")
    @patch("app.extraction.get_all.make_request")
    @patch("app.extraction.get_all.save_json")
    def test_get_teams_stats(self, mock_save_json, mock_make_request, mock_read_csv):
        mock_df = pd.DataFrame(
            {"api_parameter": ["season_id"], "season_id": [20242025]}
        )
        mock_read_csv.return_value = mock_df
        mock_data = {"teams": [{"team_id": "EDM", "team_name": "Edmonton Oilers"}]}
        mock_make_request.return_value = (mock_data, None)

        get_teams_stats()

        mock_read_csv.assert_called_once_with("data/csv_data/processed/parameters.csv")
        expected_url = (
            "https://api.nhle.com/stats/rest/en/team/summary?seasonId=20242025&TypeId=2"
        )
        mock_make_request.assert_called_once_with(expected_url)
        mock_save_json.assert_called_once_with(
            "stats_all_teams_20242025", mock_data, "data/json_data/raw_team_stats"
        )

    #######################################################################################################################

    @patch("app.extraction.get_all.pd.read_csv")
    @patch("app.extraction.get_all.make_request")
    @patch("app.extraction.get_all.save_json")
    def test_get_current_goalie_stats_leaders(
        self, mock_save_json, mock_make_request, mock_read_csv
    ):
        mock_df = pd.DataFrame(
            {"api_parameter": ["season_id"], "season_id": [20242025]}
        )
        mock_read_csv.return_value = mock_df
        mock_data = {"teams": [{"team_id": "EDM", "team_name": "Edmonton Oilers"}]}
        mock_make_request.return_value = (mock_data, None)

        get_current_goalie_stats_leaders()

        mock_read_csv.assert_called_once_with("data/csv_data/processed/parameters.csv")
        expected_url = (
            "https://api-web.nhle.com/v1/goalie-stats-leaders/20242025/2?limit=-1"
        )
        mock_make_request.assert_called_once_with(expected_url)
        mock_save_json.assert_called_once_with(
            "stats_current_goalies_20242025",
            mock_data,
            "data/json_data/raw_goalie_stats",
        )

    #######################################################################################################################

    @patch("app.extraction.get_all.pd.read_csv")
    @patch("app.extraction.get_all.make_request")
    @patch("app.extraction.get_all.save_json")
    def test_get_current_skater_stats_leaders(
        self, mock_save_json, mock_make_request, mock_read_csv
    ):
        mock_df = pd.DataFrame(
            {"api_parameter": ["season_id"], "season_id": [20242025]}
        )
        mock_read_csv.return_value = mock_df
        mock_data = {"teams": [{"team_id": "EDM", "team_name": "Edmonton Oilers"}]}
        mock_make_request.return_value = (mock_data, None)

        get_current_skater_stats_leaders()

        mock_read_csv.assert_called_once_with("data/csv_data/processed/parameters.csv")
        expected_url = (
            "https://api-web.nhle.com/v1/skater-stats-leaders/20242025/2?&limit=-1"
        )
        mock_make_request.assert_called_once_with(expected_url)
        mock_save_json.assert_called_once_with(
            "stats_current_skaters_20242025",
            mock_data,
            "data/json_data/raw_skater_stats",
        )

    #######################################################################################################################

    @patch("app.extraction.get_all.pd.read_csv")
    @patch("app.extraction.get_all.make_request")
    @patch("app.extraction.get_all.save_json")
    def test_get_team_roster_by_season(
        self, mock_save_json, mock_make_request, mock_read_csv
    ):
        mock_df = pd.DataFrame({"team_id": ["EDM"], "season_id": [20242025]})
        mock_read_csv.return_value = mock_df
        mock_data = {"teams": [{"team_id": "EDM", "team_name": "Edmonton Oilers"}]}
        mock_make_request.return_value = (mock_data, None)

        get_team_roster_by_season()

        mock_read_csv.assert_called_once_with(
            "data/csv_data/processed/parameters_team_season.csv"
        )
        expected_url = "https://api-web.nhle.com/v1/roster/EDM/20242025"
        mock_make_request.assert_called_once_with(expected_url)
        mock_save_json.assert_called_once_with(
            "roster_EDM_20242025", mock_data, "data/json_data/raw_roster_season"
        )

    #######################################################################################################################

    @patch("app.extraction.get_all.pd.read_csv")
    @patch("app.extraction.get_all.make_request")
    @patch("app.extraction.get_all.save_json")
    def test_get_club_stats_for_the_season_for_a_team(
        self, mock_save_json, mock_make_request, mock_read_csv
    ):
        mock_df = pd.DataFrame(
            {
                "triCode": ["EDM"],
            }
        )
        mock_read_csv.return_value = mock_df
        mock_data = {"teams": [{"team_id": "EDM", "team_name": "Edmonton Oilers"}]}
        mock_make_request.return_value = (mock_data, None)

        get_club_stats_for_the_season_for_a_team()

        mock_read_csv.assert_called_once_with("data/csv_data/raw/single/raw_teams.csv")
        expected_url = "https://api-web.nhle.com/v1/club-stats-season/EDM"
        mock_make_request.assert_called_once_with(expected_url)
        mock_save_json.assert_called_once_with(
            "team_season_EDM", mock_data, "data/json_data/raw_team_season"
        )

    #######################################################################################################################

    @patch("app.extraction.get_all.pd.read_csv")
    @patch("app.extraction.get_all.make_request")
    @patch("app.extraction.get_all.save_json")
    def test_get_club_stats_now(self, mock_save_json, mock_make_request, mock_read_csv):
        mock_df = pd.DataFrame(
            {
                "team_id": ["EDM"],
                "season_id": [20242025],
            }
        )
        mock_read_csv.return_value = mock_df
        mock_data = {"teams": [{"team_id": "EDM", "team_name": "Edmonton Oilers"}]}
        mock_make_request.return_value = (mock_data, None)

        get_club_stats_now()

        mock_read_csv.assert_called_once_with(
            "data/csv_data/processed/parameters_team_season.csv"
        )
        expected_url = "https://api-web.nhle.com/v1/club-stats/EDM/20242025/2"
        mock_make_request.assert_called_once_with(expected_url)
        mock_save_json.assert_called_once_with(
            "club_stats_EDM_20242025_2", mock_data, "data/json_data/raw_club_stats"
        )

    #######################################################################################################################

    @patch("app.extraction.get_all.pd.read_csv")
    @patch("app.extraction.get_all.make_request")
    @patch("app.extraction.get_all.save_json")
    def test_get_game_log(self, mock_save_json, mock_make_request, mock_read_csv):
        mock_df = pd.DataFrame(
            {
                "player_id": [8478402],
                "season_id": [20242025],
            }
        )
        mock_read_csv.return_value = mock_df
        mock_data = {"teams": [{"team_id": "EDM", "team_name": "Edmonton Oilers"}]}
        mock_make_request.return_value = (mock_data, None)

        get_game_log()

        mock_read_csv.assert_called_once_with(
            "data/csv_data/processed/parameters_players.csv"
        )
        expected_url = "https://api-web.nhle.com/v1/player/8478402/game-log/20242025/2"
        mock_make_request.assert_called_once_with(expected_url)
        mock_save_json.assert_called_once_with(
            "8478402_20242025_2", mock_data, "data/json_data/raw_game_log"
        )

    #######################################################################################################################

    @patch("app.extraction.get_all.pd.read_csv")
    @patch("app.extraction.get_all.make_request")
    @patch("app.extraction.get_all.save_json")
    def test_get_player_info(self, mock_save_json, mock_make_request, mock_read_csv):
        mock_df = pd.DataFrame(
            {
                "player_id": [8478402],
            }
        )
        mock_read_csv.return_value = mock_df
        mock_data = {"teams": [{"team_id": "EDM", "team_name": "Edmonton Oilers"}]}
        mock_make_request.return_value = (mock_data, None)

        get_player_info()

        mock_read_csv.assert_called_once_with(
            "data/csv_data/processed/parameters_players.csv"
        )
        expected_url = "https://api-web.nhle.com/v1/player/8478402/landing"
        mock_make_request.assert_called_once_with(expected_url)
        mock_save_json.assert_called_once_with(
            "player_8478402_info", mock_data, "data/json_data/raw_player_info"
        )

    #######################################################################################################################

    @patch("app.extraction.get_all.pd.read_csv")
    @patch("app.extraction.get_all.make_request")
    @patch("app.extraction.get_all.save_json")
    def test_get_skater_stats(self, mock_save_json, mock_make_request, mock_read_csv):
        mock_df = pd.DataFrame(
            {
                "api_parameter": ["season_id"],
                "season_id": [20242025],
            }
        )
        mock_read_csv.return_value = mock_df
        mock_data = {"teams": [{"team_id": "EDM", "team_name": "Edmonton Oilers"}]}
        mock_make_request.return_value = (mock_data, None)

        get_skater_stats()

        mock_read_csv.assert_called_once_with("data/csv_data/processed/parameters.csv")
        expected_url = "https://api.nhle.com/stats/rest/en/skater/summary?limit=-1&cayenneExp=seasonId=20242025"
        mock_make_request.assert_called_once_with(expected_url)
        mock_save_json.assert_called_once_with(
            "stats_current_skaters_20242025",
            mock_data,
            "data/json_data/raw_all_skater_stats",
        )

    #######################################################################################################################

    @patch("app.extraction.get_all.pd.read_csv")
    @patch("app.extraction.get_all.make_request")
    @patch("app.extraction.get_all.save_json")
    def test_get_goalie_stats(self, mock_save_json, mock_make_request, mock_read_csv):
        mock_df = pd.DataFrame(
            {
                "api_parameter": ["season_id"],
                "season_id": [20242025],
            }
        )
        mock_read_csv.return_value = mock_df
        mock_data = {"teams": [{"team_id": "EDM", "team_name": "Edmonton Oilers"}]}
        mock_make_request.return_value = (mock_data, None)

        get_goalie_stats()

        mock_read_csv.assert_called_once_with("data/csv_data/processed/parameters.csv")
        expected_url = "https://api.nhle.com/stats/rest/en/goalie/summary?limit=-1&cayenneExp=seasonId=20242025"
        mock_make_request.assert_called_once_with(expected_url)
        mock_save_json.assert_called_once_with(
            "stats_current_skaters_20242025",
            mock_data,
            "data/json_data/raw_all_goalies_stats",
        )


if __name__ == "__main__":
    unittest.main()  # Executa todos os métodos que comaçam com test_ na classe de teste.
