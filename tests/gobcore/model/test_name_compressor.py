from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.model.name_compressor import NameCompressor


MOCK_CONVERSIONS = {
    "something very special": "nothing"
}

class TestNameCompressor(TestCase):

    @patch("gobcore.model.name_compressor._CONVERSIONS",)
    def test_conversions(self, mocked_conversions):
        names = [
            "",
            "is_bron_voor_aantekening_kadastraal_object",
            "betrokken_bij_appartementsrechtsplitsing_vve",
            "ontstaan_uit_appartementsrechtsplitsing_vve",
            "some string"
            "some"
        ]

        # Don't have conversions without any profit
        for key, value in MOCK_CONVERSIONS.items():
            conversion = NameCompressor._compressed_value(value)
            self.assertTrue(len(conversion) < len(key))

        # uncompress(compress(name)) == name
        # conversions can never result in longer strings than the originale
        for name in names:
            conversion = NameCompressor.compress_name(name)
            self.assertTrue(len(conversion) <= len(name))
            self.assertEqual(NameCompressor.uncompress_name(conversion), name)

        # test for long names
        mock_print = MagicMock()
        with patch('builtins.print', mock_print):
            s = "x" * NameCompressor.LONG_NAME_LENGTH
            NameCompressor.compress_name(s)
            mock_print.assert_not_called()

            s = "x" * (NameCompressor.LONG_NAME_LENGTH + 1)
            NameCompressor.compress_name(s)
            mock_print.assert_called()
