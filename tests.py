import basketball_reference_helpers as helpers
import datetime

def test_get_season_id_from_date():
    assert helpers.get_season_id_from_date(year=2020, month=10) == 2021
    assert helpers.get_season_id_from_date('2020-10-06') == 2021
    assert helpers.get_season_id_from_date('1985', '1') == 1985
    assert helpers.get_season_id_from_date(1991, 4) == 1991
    assert helpers.get_season_id_from_date('1991-04-20') == 1991
    assert helpers.get_season_id_from_date(helpers.datetime_to_date_str(datetime.datetime(1987, 10, 6))) == 1988


def test_datetime_to_date_str():
    assert helpers.datetime_to_date_str(datetime.datetime(year=1997, month=10, day=6)) == '1997-10-06'
    assert helpers.datetime_to_date_str(datetime.datetime(1998, 1, 14)) == "1998-01-14"
    assert helpers.datetime_to_date_str(datetime.datetime(2022, 12, 6)) == "2022-12-06"


def test_get_division():
    assert helpers.get_locale('1').locale == "ATLANTIC"
    assert helpers.get_locale("Atlantic").id == 1
    assert helpers.get_locale(1).locale == "ATLANTIC"
    assert helpers.helpers.get_locale('9') is None
    assert helpers.get_locale('atlantic').id == 1
    assert helpers.get_locale(10) is None


def test_format_converter():
    assert helpers.get_format(1) == "TOTALS"
    assert helpers.get_format(2) == "PER_GAME"
    assert helpers.get_format(3) == "PER_MINUTE"
    assert helpers.get_format(4) == "PER_POSS"
    assert helpers.get_format("totals") == 1
    assert helpers.get_format("PER_GAME") == 2
    assert helpers.get_format("not valid") is None


def test_mp_handler():
    assert helpers.mp_handler('7:36') == 7.36  # test str
    assert helpers.mp_handler(7.36) == 7.36  # test float
    assert helpers.mp_handler(7) == 7  # test int
    assert helpers.mp_handler(None) is None


def test_convert_format():
    assert helpers.get_format('quarter_1').id == 1  # test with id
    assert helpers.get_format(1).format == "QUARTER_1"  # test with format name
    assert helpers.get_format("TOTALS").id == 8
    assert helpers.get_format("Nathan") is None  # test with fake format
    assert helpers.get_format("Q1").id == 1
    assert helpers.get_format(7).format == "GAME"


def test_get_career_seasons():
    assert helpers.get_career_seasons("Michael Jordan") == [1985, 2003]  # test with player name
    assert helpers.get_career_seasons(helpers.get_player("Michael Jordan").id) == [1985, 2003]  # test with player id
    assert helpers.get_career_seasons("Kobe Bryant") == [1997, 2016]
    assert helpers.get_career_seasons("LeBron James") == [2004, helpers.get_season_id_from_date(year=datetime.datetime.now().year, month=datetime.datetime.now().month)]  # test with active player
    assert helpers.get_career_seasons("Nathan Frank") == [None, None]  # test this when shot chart finishes. Should return None