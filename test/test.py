import psycopg2
import configparser
import csv

test_queries = [
    (
        """
    SELECT location AS city, COUNT(location) AS listener_count 
    FROM dwh.songplays 
    WHERE level = 'paid' 
    GROUP BY city 
    ORDER by listener_count DESC 
""",
        ("city", "listener_count"),
    ),
    (
        """
    SELECT (query_2.total_play / query_2.number_of_city) AS avg_play_each_city 
    FROM (
        SELECT SUM(query_1.listener_count) AS total_play, COUNT(city) AS number_of_city FROM (
            SELECT location AS city, COUNT(location) AS listener_count 
            FROM dwh.songplays 
            WHERE level = 'paid' 
            GROUP BY city 
            ORDER by listener_count DESC
            ) query_1
        ) query_2
    """,
        ("avg_play_each_city", ""),
    ),
]


def main():
    config = configparser.ConfigParser()
    config.read("../dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config["DB"].values())
    )

    cur = conn.cursor()

    for (i, query) in enumerate(test_queries):
        print("Query", i + 1, ":")
        print(query[0])
        try:
            cur.execute(query[0])
            conn.commit()
            with open("test_query_{}.csv".format(i + 1), "w") as csv_file:
                writer = csv.writer(csv_file, delimiter=",")
                writer.writerow(query[1])
                line = cur.fetchone()
                while line:
                    writer.writerow(line)
                    line = cur.fetchone()

        except Exception as e:
            print(e)

    conn.close()


if __name__ == "__main__":
    main()
