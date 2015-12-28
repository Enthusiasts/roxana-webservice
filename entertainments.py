# coding=UTF-8

__author__ = 'debalid'


class EntertainmentsDAO(object):
    def __init__(self, postgres):
        self.__postgres = postgres
        self.__db_rows_limit = 10000

    @staticmethod
    def query_param(ent_type):
                return {
                    "bar": "Бары",
                    "fastfood": "Предприятия быстрого обслуживания",
                    "cafe": "Кафе",
                    "cafeteria": "Кафетерии",
                    "cafe_young": "Кафе детские, молодежные и семейного досуга",
                    "coffee_house": "Кофейни",
                    "buffet": "Буфеты",
                    "cookery": "Магазины кулинарии",
                    "dining_room": "Столовые",
                    "food_plant": "Комбинаты питания",
                    "eatery": "Закусочные",
                    "catering": "Общественное питание в Москве",
                    "complex": "Единые производственно-хозяйственные комплексы предприятий питания",
                    "workshop": "Специализированные цеха"
                }[ent_type.lower()]

    def by_type(self, ent_type):
        if ent_type:

            try:
                query_actual = EntertainmentsDAO.query_param(ent_type)
            except KeyError:
                return 0, []

            with self.__postgres.one() as owner, owner.connection.cursor() as curs:
                curs.execute("SELECT * FROM entertainments WHERE type=%s LIMIT %s", (query_actual, self.__db_rows_limit))
                if curs.rowcount > 0:
                    rows = curs.fetchall()
                    # (id, title, cost, zone_title, longitude, latitude, seats_count, social_priveleges, ent_type)
                    result = list(map(
                        lambda x: {
                            "id": x[0],
                            "title": x[1],
                            "cost": x[2],
                            "zone_title": x[3],
                            "longitude": x[4],
                            "latitude": x[5],
                            "seats_count": x[6],
                            "social_priveleges": x[7],
                            "ent_type": x[8]
                        }
                        , rows))
                else:
                    result = []
                return curs.rowcount, result

    def by_type_with_photo(self, ent_type):
        if ent_type:

            try:
                query_actual = EntertainmentsDAO.query_param(ent_type)
            except KeyError:
                return 0, []

            with self.__postgres.one() as owner, owner.connection.cursor() as curs:
                curs.execute("""
                    SELECT entertainments.*, array_agg(checkins.url)
                    FROM entertainments LEFT JOIN checkins ON entertainments.id = checkins.entertainment_id
                    WHERE entertainments.type=%s
                    GROUP BY entertainments.id
                    LIMIT %s
                    """, (query_actual, self.__db_rows_limit))
                if curs.rowcount > 0:
                    rows = curs.fetchall()
                    # (id, title, cost, zone_title, longitude, latitude, seats_count, social_priveleges, ent_type)
                    result = list(map(
                        lambda x: {
                            "id": x[0],
                            "title": x[1],
                            "cost": x[2],
                            "zone_title": x[3],
                            "longitude": x[4],
                            "latitude": x[5],
                            "seats_count": x[6],
                            "social_priveleges": x[7],
                            "ent_type": x[8],
                            "instagram_urls": x[9]
                        }
                        , rows))

                else:
                    result = []
                return curs.rowcount, result

    def by_type_with_cluster_checkins(self, ent_type):
        if ent_type:

            try:
                query_actual = EntertainmentsDAO.query_param(ent_type)
            except KeyError:
                return 0, []

            with self.__postgres.one() as owner, owner.connection.cursor() as curs:
                curs.execute("\
                    SELECT entertainments.*, array_agg(checkins.url), entertainments_stats.cluster_checkins_type \
                    FROM entertainments \
                      LEFT JOIN entertainments_stats ON entertainments.id = entertainments_stats.ent_id \
                      LEFT JOIN checkins ON entertainments.id = checkins.entertainment_id \
                    WHERE entertainments.type=%s \
                    GROUP BY entertainments.id, entertainments_stats.ent_id \
                    LIMIT %s \
                    ", (query_actual, self.__db_rows_limit))
                if curs.rowcount > 0:
                    rows = curs.fetchall()
                    # (id, title, cost, zone_title, longitude, latitude, seats_count, social_priveleges, ent_type)
                    result = list(map(
                        lambda x: {
                            "id": x[0],
                            "title": x[1],
                            "cost": x[2],
                            "zone_title": x[3],
                            "longitude": x[4],
                            "latitude": x[5],
                            "seats_count": x[6],
                            "social_priveleges": x[7],
                            "ent_type": x[8],
                            "instagram_urls": x[9],
                            "cluster_checkins_type": x[10]
                        }
                        , rows))

                else:
                    result = []
                return curs.rowcount, result
