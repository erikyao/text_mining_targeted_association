import obonet
import re
import os
import csv
import random
from collections import defaultdict

from biothings_client import get_client

GENE_CLIENT = get_client('gene')
graph = obonet.read_obo("http://purl.obolibrary.org/obo/pr.obo")
pr_to_ncbigene_dict = {}


def query_pr_to_symbol(pr):
    if pr not in graph.nodes:
        return
    node_info = graph.nodes[pr]
    if "synonym" in node_info:
        for syn in node_info['synonym']:
            if 'EXACT PRO-short-label' in syn: 
                match = re.findall(r'\"(.+?)\"', syn)
                if match and len(match) > 0:
                    return match[0]
    return

def query_pr_to_uniprot(pr):
    if pr not in graph.nodes:
        return
    node_info = graph.nodes[pr]
    if "xref" in node_info:
        for xref in node_info['xref']:
            if 'UniProtKB:' in xref: 
                return xref.split(':')[-1]
    return

def query_uniprot_to_ncbigene(uniprot_ids: list) -> dict:
    """Use biothings_client.py to query uniprot ids and get back 'entrezgene' in mygene.info
    
    :param: uniprot_ids: list of uniprot ids
    """
    res = GENE_CLIENT.querymany(uniprot_ids, scopes='uniprot.Swiss-Prot', fields="entrezgene")
    new_res = defaultdict(list)
    for item in res:
        if "notfound" not in item and "entrezgene" in item and item['query'] not in new_res:
            new_res[item['query']].append(item['entrezgene'])
    return new_res

def query_symbol_to_ncbigene(symbols: list) -> dict:
    """Use biothings_client.py to query gene symbols and get back 'entrezgene' in mygene.info
    
    :param: uniprot_ids: list of gene symbols
    """
    res = GENE_CLIENT.querymany(symbols, scopes='symbol', fields="entrezgene", species="human")
    new_res = defaultdict(list)
    for item in res:
        if "notfound" not in item:
            if "entrezgene" in item:
                new_res[item['query']].append(item['entrezgene'])
    return new_res

def query_prs_to_ncbigenes(prs):
    pr_uniprot_mapping = {}
    pr_symbol_mapping = {}
    pr_ncbigene_mapping = {}
    for pr in prs:
        if not pr.startswith("PR:0"):
            uniprot = query_pr_to_uniprot(pr)
            if uniprot:
                pr_uniprot_mapping[pr] = uniprot
            else:
                print("pr {} failed to find mapping".format(pr))
        else:
            symbol = query_pr_to_symbol(pr)
            if symbol:
                pr_symbol_mapping[pr] = symbol
            else:
                print("pr {} failed to find mapping".format(pr))
    symbol_ncbigene_mapping = query_symbol_to_ncbigene(list(pr_symbol_mapping.values()))
    uniprot_ncbigene_mapping = query_uniprot_to_ncbigene(list(pr_uniprot_mapping.values()))
    for pr in pr_uniprot_mapping:
        if pr_uniprot_mapping[pr] in uniprot_ncbigene_mapping:
            pr_ncbigene_mapping[pr] = uniprot_ncbigene_mapping[pr_uniprot_mapping[pr]]
    for pr in pr_symbol_mapping:
        if pr_symbol_mapping[pr] in symbol_ncbigene_mapping:
            pr_ncbigene_mapping[pr] = symbol_ncbigene_mapping[pr_symbol_mapping[pr]]
    return pr_ncbigene_mapping


def load_data(data_folder):
    nodes_file_path = os.path.join(data_folder, "text-mined.nodes.current.kgx.tsv")
    edges_file_path = os.path.join(data_folder, "text-mined.edges.current.kgx.tsv")
    nodes_f = open(nodes_file_path)
    edges_f = open(edges_file_path)
    prs = set()
    id_type_mapping = {}
    evidence = {}
    nodes_data = csv.reader(nodes_f, delimiter="\t")
    edges_data = csv.reader(edges_f, delimiter="\t")
    for line in nodes_data:
        if line[0].startswith("PR"):
            prs.add(line[0])
        if line[2] == "biolink:GeneOrGeneProduct":
            semantic_type = "Gene"
        if line[2] == "biolink:InformationContentEntity":
            evidence[line[0]] = {
                "publications": line[3],
                "score": line[4],
                "sentence": line[5],
                "subject_spans": line[6],
                "relation_spans": line[7],
                "object_spans": line[8],
                "provided_by": line[9]
            }
        else:
            semantic_type = line[2].split(':')[-1] if line[2].startswith("biolink:") else line[2]
        id_type_mapping[line[0]] = semantic_type
    pr_ncbigene_mapping = query_prs_to_ncbigenes(list(prs))
    next(edges_data)
    for line in edges_data:
        subject_id = line[0]
        object_id = line[2]
        res = {"subject": {}, "object": {}, }
        if subject_id.startswith("PR:") and subject_id in pr_ncbigene_mapping:
            subject_id = res["subject"]["NCBIGene"] = pr_ncbigene_mapping[subject_id]
        else:
            subject_id = [subject_id]
        if object_id.startswith("PR:") and object_id in pr_ncbigene_mapping:
            object_id = res["object"]["NCBIGene"] = pr_ncbigene_mapping[object_id]
        else:
            object_id = [object_id]
        evidence_ids = line[-1].split("|")
        evidences = [evidence[item] for item in evidence_ids]
        for s_id in subject_id:
            for o_id in object_id:
                res.update({
                    "subject": {
                        "id": s_id,
                        line[0].split(':')[0]: line[0],
                        "type": id_type_mapping[line[0]]
                    },
                    "association": {
                        "edge_label": line[1].split(':')[-1],
                        "relation": line[3],
                        "evidence": evidences,
                        "evidence_count": line[-2]
                    },
                    "object": {
                        "id": o_id,
                        line[2].split(':')[0]: line[2],
                        "type": id_type_mapping[line[2]]
                    }
                })
                if s_id != line[0]:
                    res['subject']['NCBIGene'] = s_id
                    res['subject']['id'] = 'NCBIGene:' + s_id
                if o_id != line[2]:
                    res['object']['NCBIgene'] = o_id
                    res['object']['id'] = 'NCBIGene:' + o_id
                res["_id"] = res['subject']['id'] + '-' + res['object']['id'] + '-' + str(res['association']['ngd'])
                # res["combos"] = [res['subject']['id'] + '-' + res['object']['id'], res['object']['id'] + '-' + res['subject']['id']]
                yield res