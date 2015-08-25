import pyhs2
import datetime

with pyhs2.connect(host='cq02-softwareresearch-search13.cq02.baidu.com',
                   port=10000,
                   authMechanism="PLAIN",
                    user="hdfs") as conn:
    with conn.cursor() as cur:
        # import pdb;pdb.set_trace()
        # Show databases
        print cur.getDatabases()
        print cur.getSchema()
        # Execute query
        hql = "select * from report.data \
          where stat_date = '20150317' and cmdidkey = '9_825' limit 10"
        print hql
        cur.execute(hql)

        # Return column info from query
        #print cur.getSchema()

        # Fetch table results
        results = cur.ifetch()
        print cur.getSchema()
        #import pdb;pdb.set_trace()
        for i in results:
            print i