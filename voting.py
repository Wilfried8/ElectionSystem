import random
import time

import psycopg2
from confluent_kafka import SerializingProducer, Consumer, KafkaError
import simplejson as json
from datetime import datetime

configuration = {
    'bootstrap.servers': 'localhost:9092'
}

consumer = Consumer(configuration | {
    'group.id': 'voting_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer(conf=configuration)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

if __name__ == "__main__":

    try:
        connection = psycopg2.connect("host=localhost port=5532 dbname=voting user=postgres password=postgres")
        cursor = connection.cursor()

        candidate_query = cursor.execute(
            """
                SELECT row_to_json(t, true)
                FROM (
                    SELECT * FROM candidates
                    ) t
            """
        )
        candidates = cursor.fetchall()
        candidates = [candidate[0] for candidate in candidates]
        #print(type(candidates))

        if len(candidates) == 0:
            raise Exception("No candidates found in the db")
        else:
            print(candidates)

        consumer.subscribe(
            ['voters_topic']
        )

        try:
            while True:
                msg = consumer.poll()
                if msg is None:
                    continue
                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break
                else:
                    print(" 1 lecture des vote ....")
                    voter = json.loads(msg.value().decode('utf-8'))
                    print(" 2 lecture des vote ....")
                    chosen_candidate = random.choice(candidates)
                    print(type(voter))
                    print(type(chosen_candidate))


                    vote = {**voter, **chosen_candidate, 'voting_time': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                            'vote': 1}

                    # vote = voter | chosen_candidate | {
                    #     'voting_time': datetime.utcnow().strftime("Y-%m-%d %H:%M:%S"),
                    #     'vote': 1
                    # }
                    print(" 3 lecture des vote ....")
                    try:
                        print('user {} is voting for candidate : {}'.format(vote['voter_id'], vote['candidate_id']))

                        cursor.execute(
                            """
                                INSERT INTO votes(voter_id, candidate_id, voting_time)
                                VALUES(%s, %s, %s)
                            """, (vote['voter_id'], vote['candidate_id'], vote['voting_time'])
                        )

                        connection.commit()

                        producer.produce(
                            topic='vote_topic',
                            key=vote['voter_id'],
                            value=json.dumps(vote),
                            on_delivery=delivery_report
                        )
                        producer.poll(0)

                    except Exception as e:
                        print(e)

                time.sleep(0.5)

        except Exception as e:
            print(e)

    except Exception as e:
        print(f"we have a probleme with the connection to posgres due to : {e}")