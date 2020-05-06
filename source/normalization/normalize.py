import shutil
import time
import pandas
from rdflib import Namespace
from rdflib import Graph
from rdflib import URIRef, BNode, Literal
from rdflib.namespace import RDF, FOAF
import os
import re
import csv
import sys
import uuid
import rdflib
import getopt
import subprocess
from rdflib.plugins.sparql import prepareQuery
from configparser import ConfigParser, ExtendedInterpolation
from normalization.triples_map import TriplesMap as tm
import traceback
from mysql import connector
from concurrent.futures import ThreadPoolExecutor
import time
import json
import xml.etree.ElementTree as ET
import psycopg2

from datetime import datetime

from .functions import *

try:
	from triples_map import TriplesMap as tm
except:
    from .triples_map import TriplesMap as tm    

global g_triples 
g_triples = {}
global number_triple
number_triple = 0
global triples
triples = []
global duplicate
duplicate = ""
global start_time
start_time = 0
global user, password, port, host
user, password, port, host = "", "", "", ""
global join_table 
join_table = {}
global po_table
po_table = {}
global enrichment
enrichment = ""

v_csv_output_path = "C:\\Users\\TorabinejadM\\Desktop\\Thesis\\implementation\\sdm-tib-github\\rml-normalizer\\experiments\\csv\\"
v_mapping_output_path="C:\\Users\\TorabinejadM\\Desktop\\Thesis\\implementation\\sdm-tib-github\\rml-normalizer\\experiments\\mappings\\"

def mapping_parser(mapping_file):

    """
    (Private function, not accessible from outside this package)

    Takes a mapping file in Turtle (.ttl) or Notation3 (.n3) format and parses it into a list of
    TriplesMap objects (refer to TriplesMap.py file)

    Parameters
    ----------
    mapping_file : string
        Path to the mapping file

    Returns
    -------
    A list of TriplesMap objects containing all the parsed rules from the original mapping file
    """

    mapping_graph = rdflib.Graph()

    try:
        print("_________test_________")
        print(mapping_file)
        mapping_graph.load(mapping_file, format='n3')
        print("_________test_________")
    except Exception as n3_mapping_parse_exception:
        print(n3_mapping_parse_exception)
        print('Could not parse {} as a mapping file'.format(mapping_file))
        print('Aborting...')
        sys.exit(1)
    
    mapping_query = """
        prefix rr: <http://www.w3.org/ns/r2rml#> 
        prefix rml: <http://semweb.mmlab.be/ns/rml#> 
        prefix ql: <http://semweb.mmlab.be/ns/ql#> 
        prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> 
        SELECT DISTINCT *
        WHERE {

    # Subject -------------------------------------------------------------------------
            ?triples_map_id rml:logicalSource ?_source .
            OPTIONAL{?_source rml:source ?data_source .}
            OPTIONAL {?_source rml:referenceFormulation ?ref_form .}
            OPTIONAL { ?_source rml:iterator ?iterator . }
            OPTIONAL { ?_source rr:tableName ?tablename .}
            OPTIONAL { ?_source rml:query ?query .}
            
            ?triples_map_id rr:subjectMap ?_subject_map .
            OPTIONAL {?_subject_map rr:template ?subject_template .}
            OPTIONAL {?_subject_map rml:reference ?subject_reference .}
            OPTIONAL {?_subject_map rr:constant ?subject_constant}
            OPTIONAL { ?_subject_map rr:class ?rdf_class . }
            OPTIONAL { ?_subject_map rr:termType ?termtype . }
            OPTIONAL { ?_subject_map rr:graph ?graph . }
            OPTIONAL { ?_subject_map rr:graphMap ?_graph_structure .
                       ?_graph_structure rr:constant ?graph . }
            OPTIONAL { ?_subject_map rr:graphMap ?_graph_structure .
                       ?_graph_structure rr:template ?graph . }           

    # Predicate -----------------------------------------------------------------------
            OPTIONAL {
            ?triples_map_id rr:predicateObjectMap ?_predicate_object_map .
            
            OPTIONAL {
                ?triples_map_id rr:predicateObjectMap ?_predicate_object_map .
                ?_predicate_object_map rr:predicateMap ?_predicate_map .
                ?_predicate_map rr:constant ?predicate_constant .
            }
            OPTIONAL {
                ?_predicate_object_map rr:predicateMap ?_predicate_map .
                ?_predicate_map rr:template ?predicate_template .
            }
            OPTIONAL {
                ?_predicate_object_map rr:predicateMap ?_predicate_map .
                ?_predicate_map rml:reference ?predicate_reference .
            }
            OPTIONAL {
                ?_predicate_object_map rr:predicate ?predicate_constant_shortcut .
             }
            

    # Object --------------------------------------------------------------------------
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?_object_map .
                ?_object_map rr:constant ?object_constant .
                OPTIONAL {
                    ?_object_map rr:datatype ?object_datatype .
                }
            }
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?_object_map .
                ?_object_map rr:template ?object_template .
                OPTIONAL {?_object_map rr:termType ?term .}
                OPTIONAL {
                    ?_object_map rr:datatype ?object_datatype .
                }
            }
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?_object_map .
                ?_object_map rml:reference ?object_reference .
                OPTIONAL { ?_object_map rr:language ?language .}
                OPTIONAL {
                    ?_object_map rr:datatype ?object_datatype .
                }
            }
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?_object_map .
                ?_object_map rr:parentTriplesMap ?object_parent_triples_map .
                OPTIONAL {
                    ?_object_map rr:joinCondition ?join_condition .
                    ?join_condition rr:child ?child_value;
                                 rr:parent ?parent_value.
                }
                OPTIONAL {
                    ?_object_map rr:joinCondition ?join_condition .
                    ?join_condition rr:child ?child_value;
                                 rr:parent ?parent_value;
                }
            }
            OPTIONAL {
                ?_predicate_object_map rr:object ?object_constant_shortcut .
                OPTIONAL {
                    ?_object_map rr:datatype ?object_datatype .
                }
            }
            }
            OPTIONAL {
                ?_source a d2rq:Database;
                  d2rq:jdbcDSN ?jdbcDSN; 
                  d2rq:jdbcDriver ?jdbcDriver; 
                d2rq:username ?user;
                d2rq:password ?password .
            }
        } """

    mapping_query_results = mapping_graph.query(mapping_query)
    triples_map_list = []
    
    for result_triples_map in mapping_query_results:
        triples_map_exists = False
        
        for triples_map in triples_map_list:
            triples_map_exists = triples_map_exists or (str(triples_map.triples_map_id) == str(result_triples_map.triples_map_id))
        
        if not triples_map_exists:
            if result_triples_map.subject_template is not None:
                if result_triples_map.rdf_class is None:
                    reference, condition = string_separetion(str(result_triples_map.subject_template))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_template), condition, "template2", result_triples_map.rdf_class, result_triples_map.termtype, result_triples_map.graph)
                else:
                    reference, condition = string_separetion(str(result_triples_map.subject_template))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_template), condition, "template2", str(result_triples_map.rdf_class), result_triples_map.termtype, result_triples_map.graph)
            elif result_triples_map.subject_reference is not None:
                if result_triples_map.rdf_class is None:
                    reference, condition = string_separetion(str(result_triples_map.subject_reference))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_reference), condition, "reference", result_triples_map.rdf_class, result_triples_map.termtype, result_triples_map.graph)
                else:
                    reference, condition = string_separetion(str(result_triples_map.subject_reference))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_reference), condition, "reference", str(result_triples_map.rdf_class), result_triples_map.termtype, result_triples_map.graph)
            elif result_triples_map.subject_constant is not None:
                if result_triples_map.rdf_class is None:
                    reference, condition = string_separetion(str(result_triples_map.subject_constant))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_constant), condition, "constant", result_triples_map.rdf_class, result_triples_map.termtype, result_triples_map.graph)
                else:
                    reference, condition = string_separetion(str(result_triples_map.subject_constant))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_constant), condition, "constant", str(result_triples_map.rdf_class), result_triples_map.termtype, result_triples_map.graph)
                
            mapping_query_prepared = prepareQuery(mapping_query)

            mapping_query_prepared_results = mapping_graph.query(mapping_query_prepared, initBindings={'triples_map_id': result_triples_map.triples_map_id})
            predicate_object_maps_list = []
            
            for result_predicate_object_map in mapping_query_prepared_results:
                if result_predicate_object_map.predicate_constant is not None:
                    predicate_map = tm.PredicateMap("constant", str(result_predicate_object_map.predicate_constant), "")
                elif result_predicate_object_map.predicate_constant_shortcut is not None:
                    predicate_map = tm.PredicateMap("constant shortcut", str(result_predicate_object_map.predicate_constant_shortcut), "")
                elif result_predicate_object_map.predicate_template is not None:
                    template, condition = string_separetion(str(result_predicate_object_map.predicate_template))
                    predicate_map = tm.PredicateMap("template2", template, condition)
                elif result_predicate_object_map.predicate_reference is not None:
                    reference, condition = string_separetion(str(result_predicate_object_map.predicate_reference))
                    predicate_map = tm.PredicateMap("reference", reference, condition)
                else:
                    predicate_map = tm.PredicateMap("None", "None", "None")

                if result_predicate_object_map.object_constant is not None:
                    object_map = tm.ObjectMap("constant", str(result_predicate_object_map.object_constant), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
                elif result_predicate_object_map.object_template is not None:
                    object_map = tm.ObjectMap("template2", str(result_predicate_object_map.object_template), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
                elif result_predicate_object_map.object_reference is not None:
                    object_map = tm.ObjectMap("reference", str(result_predicate_object_map.object_reference), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
                elif result_predicate_object_map.object_parent_triples_map is not None:
                    object_map = tm.ObjectMap("parent triples map", str(result_predicate_object_map.object_parent_triples_map), str(result_predicate_object_map.object_datatype), str(result_predicate_object_map.child_value), str(result_predicate_object_map.parent_value), result_predicate_object_map.term, result_predicate_object_map.language)
                elif result_predicate_object_map.object_constant_shortcut is not None:
                    object_map = tm.ObjectMap("constant shortcut", str(result_predicate_object_map.object_constant_shortcut), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
                else:
                    object_map = tm.ObjectMap("None", "None", "None", "None", "None", "None", "None")

                predicate_object_maps_list += [tm.PredicateObjectMap(predicate_map, object_map)]
                
            current_triples_map = tm.TriplesMap(str(result_triples_map.triples_map_id), str(result_triples_map.data_source), subject_map, predicate_object_maps_list, ref_form=str(result_triples_map.ref_form), iterator=str(result_triples_map.iterator), tablename=str(result_triples_map.tablename), query=str(result_triples_map.query))
            triples_map_list += [current_triples_map]

    return triples_map_list
        

def fd_parser(mapping_file):
    mapping_graph = rdflib.Graph()

    try:
        mapping_graph.load(mapping_file, format='n3')
    except Exception as n3_mapping_parse_exception:
        print(n3_mapping_parse_exception)
        print('Could not parse {} as a mapping file'.format(mapping_file))
        print('Aborting...')
        sys.exit(1)
        
    fd_query="""
        prefix rml: <http://semweb.mmlab.be/ns/rml#>
        SELECT DISTINCT *
        WHERE {
            ?triples_map_id rml:key ?_key.
            ?_key rml:column_name ?column_name.
            OPTIONAL {?_key rml:determine ?_attr.
                                                    ?_attr rml:column_name ?attr_name.
            OPTIONAL {?_attr rml:dependant ?dep_name}
            }
            }"""

    mapping_query_results = mapping_graph.query(fd_query)
        
    whole_list={}
    transitive_list={}
    for result_triples_map in mapping_query_results:
        whole_list[str(result_triples_map.attr_name)]= str(result_triples_map.attr_name)
        if result_triples_map.dep_name is not None:
            transitive_list[str(result_triples_map.attr_name)]= str(result_triples_map.dep_name)
    
    return transitive_list
        

def normalize_rules(triples_map, fd_file, data_source, output_path,mapping_file):
    try:
        fd_list=fd_file
        new_rml_graph = Graph()
        
        #base_uri='http://semweb.mmlab.be/'
        base_uri=Namespace("http://tib.de/ontario/mapping#")
        rml=Namespace("http://semweb.mmlab.be/ns/rml#")
        ql= Namespace("http://semweb.mmlab.be/ns/ql#")
        r2rml= Namespace("http://www.w3.org/ns/r2rml#")
        mThesis= Namespace("http://mThesis.eu/vocab/")
        mapping_file_name=os.path.splitext((os.path.basename(mapping_file)))[0]
        new_rml_graph.parse(mapping_file, format='n3',publicID=base_uri)
        
        base_obj=new_rml_graph.value(subject=None,predicate=rml['referenceFormulation'], object=ql['CSV'])
        base=new_rml_graph.value(subject=None,predicate=rml['logicalSource'], object=base_obj)
        new_rml_file = v_mapping_output_path+mapping_file_name+"_normal.ttl"
        deselected_cols=[]
        logial_source_node=new_rml_graph.value(subject=None,predicate=rml['referenceFormulation'], object=ql['CSV'])
        for predicate_object_map in triples_map.predicate_object_maps_list:
                if predicate_object_map.object_map.mapping_type == "template":
                        object_value = re.search("{(.+?)}",predicate_object_map.object_map.value).group(1)
                elif predicate_object_map.object_map.mapping_type == "reference":
                        object_value = predicate_object_map.object_map.value
                join_condition = fd_list.get(object_value)
                if join_condition is not None:
                    new_source_file = v_csv_output_path+mapping_file_name+"-PT_{}.csv"
                    new_source_file = new_source_file.format(join_condition+object_value)
                    selected_cols =[ join_condition, object_value]
                    df_new_source = pandas.read_csv(data_source,usecols=selected_cols)
                    df_new_source.drop_duplicates(keep = 'first', inplace = True)
                    df_new_source = df_new_source[selected_cols]
                    df_new_source.to_csv(new_source_file, index=False)
                    deselected_cols.append(object_value)
                    new_triplesmap_uri = URIRef("#"+str(object_value))
                    new_source = BNode()
                    new_subject = BNode()
                    #predicate = BNode()
                    #object = BNode()
                    new_join_condition=BNode()
                    
                    new_rml_graph.namespace_manager.bind('rml', rml, override = True, replace=True)
                    new_rml_graph.namespace_manager.bind('ql', ql, override = True, replace=True)
                    new_rml_graph.namespace_manager.bind('rr', r2rml, override = True, replace=True)
                    new_rml_graph.namespace_manager.bind('mThesis', mThesis, override = True, replace=True)
                    
                    new_rml_graph.add( (new_triplesmap_uri, RDF.type,r2rml['TriplesMap'] ) )
                    
                    new_rml_graph.add( (new_triplesmap_uri, rml['logicalSource'], new_source) )
                    new_rml_graph.add( (new_source, rml['source'], Literal(new_source_file)) )
                    new_rml_graph.add( (new_source, rml['referenceFormulation'], ql['CSV']) )
                    new_rml_graph.add( (new_triplesmap_uri, r2rml['subjectMap'], new_subject) )
                    
                    #to make the joins work for rocketrml, I need to ignore {}
                    new_subject_term=Literal("mThesis:"+object_value)
                    #new_subject_term=Literal(object_value)
                    #new_subject_term=Literal("{"+object_value+"}")
                    
                    #to make the joins work for rocketrml, I need to change template to reference
                    new_rml_graph.add( (new_subject, r2rml['template'], new_subject_term) )
                    #new_rml_graph.add( (new_subject, r2rml['reference'], new_subject_term) )
                    #new_rml_graph.add( (new_subject, r2rml['template'], new_subject_term) )
                    
                    object_map_node=new_rml_graph.value(subject=None,predicate=rml['reference'], object=Literal(object_value))                        
                    new_rml_graph.add( (object_map_node, r2rml['parentTriplesMap'], new_triplesmap_uri) )
                    new_rml_graph.add( (object_map_node, r2rml['joinCondition'], new_join_condition) )
                    new_rml_graph.add( (new_join_condition, r2rml['child'], Literal(join_condition)) )
                    new_rml_graph.add( (new_join_condition, r2rml['parent'], Literal(join_condition)) )
                    new_rml_graph.remove( (object_map_node, rml['reference'], Literal(object_value)) )
                    
                    print("________________________________creating new triples map________________________________")
                        
        selected_cols=[]
        mapping_file_name=mapping_file_name+"-"+"CT_"
        selected_cols.append(re.search("{(.+?)}",triples_map.subject_map.value).group(1))
        mapping_file_name=mapping_file_name+selected_cols[0]
        for pom_item in triples_map.predicate_object_maps_list:
            selected_cols.append(pom_item.object_map.value)
            mapping_file_name=mapping_file_name+pom_item.object_map.value
            
        new_source_file = v_csv_output_path+mapping_file_name+".csv"
        df_new_source = pandas.read_csv(triples_map.data_source,usecols=selected_cols)
        df_new_source.drop_duplicates(keep = 'first', inplace = True)
        df_new_source = df_new_source[selected_cols]
        df_new_source.to_csv(new_source_file, index=False)
        
        old_source_file=new_rml_graph.value(subject=logial_source_node,predicate=rml['source'], object=None)
        new_rml_graph.add( (logial_source_node, rml['source'], Literal(new_source_file)) )
        new_rml_graph.remove( (logial_source_node, rml['source'], Literal(old_source_file)) )
        new_rml_graph.serialize(destination=new_rml_file, format='n3',base="http://tib.de/ontario/mapping#")
        shutil.copyfile(old_source_file, v_csv_output_path+os.path.basename(old_source_file))
        shutil.copyfile(mapping_file, v_mapping_output_path+os.path.basename(mapping_file))
    except Exception as e:
        print(e)

def normalize(config_path,csv_output_path=".\\experiments\\csv\\",mapping_output_path=".\\experiments\\mappings\\"):
        global v_csv_output_path
        v_csv_output_path=csv_output_path
        v_mapping_output_path=mapping_output_path
        dt_format='%d.%m.%Y %H:%M:%S,%f'
        dt_now = datetime.datetime.now().strftime(dt_format)
        print("started at:",dt_now)
        config = ConfigParser(interpolation=ExtendedInterpolation())
        config.read(config_path)
        output_path = config["datasets"]["output_folder"]
        with ThreadPoolExecutor(max_workers=10) as executor:
            for dataset_number in range(int(config["datasets"]["number_of_datasets"])):
                dataset_i = "dataset" + str(int(dataset_number) + 1)
                print(config[dataset_i]["mapping"])
                mapping_file=config[dataset_i]["mapping"]
                triples_map_list = mapping_parser(mapping_file)
                
                fd_list = fd_parser(config[dataset_i]["closure"])
                for triples_map in triples_map_list:
                    with open(str(triples_map.data_source)) as data_source:
                        data = csv.DictReader(data_source, delimiter=',')
                        print("---------------------- Normalization started ... ----------------------")
                        executor.submit(normalize_rules,triples_map,fd_list,triples_map.data_source,output_path,mapping_file)
                        print("---------------------- Normalization done! ----------------------")
        
        dt_now = datetime.datetime.now().strftime(dt_format)
        print("finished at:",dt_now)                                            
def main():
    normalize(config_path)

if __name__ == "__main__":
    main()