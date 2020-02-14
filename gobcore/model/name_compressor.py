# Converted table names
_CONVERSIONS = {
    "is_bron_voor_aantekening_kadastraal_object": "bron_kad_obj",
    "ontstaan_uit_appartementsrechtsplitsing_vve": "ontst_apprechtsplit_vve",
    "betrokken_bij_appartementsrechtsplitsing_vve": "betr_apprechtsplit_vve",
}


class NameCompressor():
    # Use the name compressor for all table names that are too long
    #
    # for PostgreSQL the maximimum length = 63
    # But simply testing for this length is not enough
    #
    # The name is also used with prefixes:
    # - mv_ for materialized views
    # And postfixes
    # _tmp for temporary tables
    #

    # Warn if name exceeds this length
    LONG_NAME_LENGTH = 55

    @classmethod
    def _compressed_value(cls, value):
        return f"_{value}_"

    @classmethod
    def compress_name(cls, name):
        for src, dst in _CONVERSIONS.items():
            name = name.replace(src, cls._compressed_value(dst))

        if len(name) > cls.LONG_NAME_LENGTH:
            print(f"WARNING: LONG NAME: {name} ({len(name)})")

        return name

    @classmethod
    def uncompress_name(cls, name):
        for src, dst in _CONVERSIONS.items():
            name = name.replace(cls._compressed_value(dst), src)
        return name
