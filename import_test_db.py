import subprocess
import config
def import_test_db():
    # mongoexport -h 127.0.0.1 - -port 32768 - d tweet_db_rest - c tweet_collection - o ~ / Desktop / tweet_collection1.json
    subprocess.call("mongoimport -h "+config.address+" --port "+config.port+" -d tweet_db_rest -c tweet_collection ./rest_db.json", shell=True)
    subprocess.call("mongoimport -h " + config.address + " --port " + config.port + " -d tweet_db_stream -c tweet_collection ./stream_db.json", shell=True)
