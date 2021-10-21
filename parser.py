import os
import csv
import json

EXCLUDE_LIST = ['CHEBI:35222', 'CHEBI:23888', 'CHEBI:36080', 'PR:000003944', 'PR:000011336', 'CL:0000000',
                'PR:000000001', 'HP:0045088', 'HP:0001259', 'HP:0041092', 'HP:0031796', 'HP:0011011', 'HP:0001056',
                'HP:0011010', 'MONDO:0021141', 'MONDO:0021152', 'HP:0000005', 'HP:0000005', 'MONDO:0017169',
                'MONDO:0024497', 'MONDO:0000605', 'HP:0040285', 'HP:0025304', 'HP:0030645', 'HP:0025279',
                'HP:0003676', 'HP:0030649', 'HP:0012835', 'HP:0003674', 'HP:0020034', 'HP:0002019', 'HP:0040282',
                'HP:0040279', 'HP:0040279', 'HP:0032322', 'HP:0030645', 'HP:0011009', 'HP:0012829', 'HP:0030645',
                'HP:0031375', 'HP:0030650', 'HP:0011009', 'HP:0012824', 'HP:0012828', 'HP:0012828', 'HP:0025287',
                'HP:0025145', 'HP:0003676', 'HP:0003676', 'HP:0030645', 'MONDO:0005070', 'HP:0002664', 'MONDO:0021178',
                'MONDO:0021137', 'MONDO:0002254', 'MONDO:0021136', 'HP:0012838', 'HP:0003680', 'HP:0031915',
                'HP:0012837', 'HP:0040282', 'HP:0040279', 'HP:0040279', 'HP:0012840', 'HP:0410291', 'HP:0012830',
                'HP:0025275', 'HP:0012831', 'HP:0012831', 'HP:0030646', 'MONDO:0021137', 'HP:0040279', 'HP:0040282',
                'HP:0040282', 'HP:0040279', 'HP:0040282', 'HP:0040282', 'HP:0003680', 'HP:0012838', 'HP:0012834',
                'HP:0200034', 'HP:0012825', 'HP:0040283', 'HP:0012824', 'HP:0012828', 'HP:0012828', 'HP:0100754',
                'HP:0032320', 'HP:0030212', 'HP:0012826', 'HP:0003680']


def get_attribute_object(blob, atrribute_type_id) -> dict:
    for obj in blob:
        if obj['attribute_type_id'] == atrribute_type_id:
            return obj


def get_attribute_list(blob, attribute_type_id) -> list:
    object_list = []
    for obj in blob:
        if obj['attribute_type_id'] == attribute_type_id:
            object_list.append(obj)
    return object_list


def get_evidence_list(supporting_studies) -> list:
    evidence_list = []
    for study in supporting_studies:
        publication = get_attribute_object(study["attributes"], "biolink:supporting_document")["value"]
        score = get_attribute_object(study["attributes"], "biolink:extraction_confidence_score")["value"]
        sentence = get_attribute_object(study["attributes"], "biolink:supporting_text")["value"]
        subject_span = get_attribute_object(study["attributes"], "biolink:subject_location_in_text")["value"]
        object_span = get_attribute_object(study["attributes"], "biolink:object_location_in_text")["value"]
        evidence = {
            "publications": publication,
            "score": score,
            "sentence": sentence,
            "subject_spans": subject_span,
            "object_spans": object_span,
            "provided_by": study["attribute_source"]
        }
        evidence_list.append(evidence)
    return evidence_list


def load_nodes(file_path) -> dict:
    nodes_data = {}
    with open(file_path, 'r') as file_handle:
        reader = csv.reader(file_handle, delimiter='\t')
        for row in reader:
            nodes_data[row[0]] = (row[1], row[2])
    return nodes_data


def load_data(data_folder):
    entity_dict = load_nodes(os.path.join(data_folder, "nodes.tsv"))
    edges_file_path = os.path.join(data_folder, "edges.tsv")
    # this is a list of ID types where we want the prefix included
    prefix_list = ['RHEA', 'GO', 'CHEBI', 'HP', 'MONDO', 'DOID', 'EFO', 'UBERON', 'MP', 'CL', 'MGI']
    with open(edges_file_path, 'r') as file_handle:
        reader = csv.reader(file_handle, delimiter='\t')
        for line in reader:
            if line[0].upper() in EXCLUDE_LIST or line[2].upper() in EXCLUDE_LIST:
                continue
            attributes_blob = json.loads(line[-1])
            supporting_studies = get_attribute_list(attributes_blob, 'biolink:supporting_study_result')
            evidences = get_evidence_list(supporting_studies)
            subject_parts = line[0].split(':')
            object_parts = line[2].split(':')
            short_predicate = 'false'
            if 'positively' in line[1]:
                short_predicate = 'positive'
            elif 'negatively' in line[1]:
                short_predicate = 'negative'
            yield {
                "_id": f"{line[3]}-{short_predicate}",
                "subject": {
                    "id": line[0],
                    subject_parts[0]: line[0] if subject_parts[0] in prefix_list else subject_parts[1],
                    "type": entity_dict[line[0]][1].split(':')[-1]
                },
                "association": {
                    "edge_label": line[1].split(':')[-1],
                    "evidence_count": get_attribute_object(attributes_blob, "biolink:has_evidence_count")["value"],
                    "evidence": evidences,
                    "edge_attributes": json.loads(line[-1], parse_float=str, parse_int=str)
                },
                "object": {
                    "id": line[2],
                    object_parts[0]: line[2] if object_parts[0] in prefix_list else object_parts[1],
                    "type": entity_dict[line[2]][1].split(':')[-1]
                },
            }


def targeted_mapping(cls):
    return {
        "association": {
            "properties": {
                "edge_label": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                },
                "evidence_count": {
                    "type": "integer"
                },
                "evidence": {
                    "properties": {
                        "publications": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword"
                        },
                        "score": {
                            "type": "float"
                        },
                        "subject_spans": {
                            "type": "text"
                        },
                        "object_spans": {
                            "type": "text"
                        },
                        "provided_by": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword"
                        },
                        "sentence": {
                            "type": "text"
                        }
                    }
                },
                "edge_attributes": {
                    "properties": {
                        "attribute_type_id": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword"
                        },
                        "value": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword"
                        },
                        "value_type_id": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword"
                        },
                        "attribute_source": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword"
                        },
                        "attributes": {
                            "properties": {
                                "attribute_type_id": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword"
                                },
                                "value_type_id": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword"
                                },
                                "attribute_source": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword"
                                },
                                "value_url": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword"
                                },
                                "value": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword"
                                },
                                "description": {
                                    "type": "text"
                                }
                            }
                        },
                        "description": {
                            "type": "text"
                        }
                    }
                }
            }
        },
        "object": {
            "properties": {
                "id": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                },
                "UniProtKB": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                },
                "type": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                },
                "MONDO": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                },
                "HP": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                },
                "DRUGBANK": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                },
                "CHEBI": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                }
            }
        },
        "subject": {
            "properties": {
                "id": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                },
                "DRUGBANK": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                },
                "type": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                },
                "UniProtKB": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                },
                "CHEBI": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                },
                "MONDO": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                }
            }
        }
    }
