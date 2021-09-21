  
def get_release(self):
    # hard-coded release for demo purpose
    # "self" is a dumper instance, see:
    # https://github.com/biothings/biothings.api/blob/master/biothings/hub/dataload/dumper.py
    import requests
    res = requests.get("https://storage.googleapis.com/translator-text-workflow-dev-public/kgx/UniProt/nodes.tsv")
    try:
        last_modified = res.headers.get("Last-Modified", "1.0")
        return last_modified
    except:
        return "1.0"
