import os
import csv
import json


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
    with open(edges_file_path, 'r') as file_handle:
        reader = csv.reader(file_handle, delimiter='\t')
        for line in reader:
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
                    subject_parts[0]: subject_parts[1],
                    "type": entity_dict[line[0]][1].split(':')[-1]
                },
                "association": {
                    "edge_label": line[1].split(':')[-1],
                    "evidence_count": get_attribute_object(attributes_blob, "biolink:has_evidence_count")["value"],
                    "evidence": evidences,
                    "edge_attributes": json.loads(line[-1])
                },
                "object": {
                    "id": line[2],
                    object_parts[0]: object_parts[1],
                    "type": entity_dict[line[2]][1].split(':')[-1]
                },
            }
