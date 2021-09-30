  
def get_release(self):
    # hard-coded release for demo purpose
    # "self" is a dumper instance, see:
    # https://github.com/biothings/biothings.api/blob/master/biothings/hub/dataload/dumper.py
    import requests
    import datetime
    res = requests.get("https://storage.googleapis.com/translator-text-workflow-dev-public/kgx/UniProt/nodes.tsv.gz")
    try:
        last_modified = res.headers.get("Last-Modified", "1.1")
        dt = datetime.datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z')
        return dt.date().isoformat()
    except:
        return "1.1"
