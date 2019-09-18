import sqlite3
import pandas as pd

timeframes = ['2012-04']

for timeframe in timeframes:
    connection = sqlite3.connect('{}.db'.format(timeframe))
    c = connection.cursor()
    limit = 5000    # How many rows to enter in pandas data frame
    last_unix = 0   # Unix Timestamp, keep track of what data we have already trained on
    cur_length = limit
    counter = 0
    test_done = False
    while cur_length == limit:
        df = pd.read_sql(
            "SELECT * FROM parent_reply WHERE unix > {} AND parent NOT NULL AND score > 0 ORDER BY unix ASC LIMIT {}".format(last_unix, limit), connection)
        # Timestamp of the last reply
        last_unix = df.tail(1)['unix'].values[0]
        cur_length = len(df)
        # Test if data is ok before training on entire set
        if not test_done:
            # Append Parent comments to test.from
            with open("test.from", 'a', encoding='utf8') as f:
                for content in df['parent'].values:
                    f.write(content+'\n')
            # Append replies to test.to
            with open("test.to", 'a', encoding='utf8') as f:
                for content in df['comment'].values:
                    f.write(content+'\n')

            # Test files are completed
            test_done = True
        else:
            # Train on entie set of data
            # Append Parent comments to train.from
            with open("train.from", 'a', encoding='utf8') as f:
                for content in df['parent'].values:
                    f.write(content+'\n')
            # Append replies to train.to
            with open("train.to", 'a', encoding='utf8') as f:
                for content in df['comment'].values:
                    f.write(content+'\n')
        counter += 1
        if counter % 20 == 0:
            print(counter*limit, ' rows completed')
