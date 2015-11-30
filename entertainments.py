__author__ = 'debalid'


class EntertainmentsDAO(object):
    def __init__(self, postgres):
        self.__postgres = postgres

    def by_type(self, ent_type):
        if ent_type:
            with self.__postgres.one() as owner, owner.connection.cursor() as curs:
                curs.execute("SELECT * FROM entertainments WHERE type=%s", ent_type)
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
                return result
