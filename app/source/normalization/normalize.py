import math
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
from .triples_map import TriplesMap as tm
import traceback
from mysql import connector
from concurrent.futures import ThreadPoolExecutor
import time
import json
import xml.etree.ElementTree as ET
import psycopg2

import matplotlib.pyplot as plt
import numpy as np
from textwrap import wrap

from .functions import *

try:
	from triples_map import TriplesMap as tm
except:
    from .triples_map import TriplesMap as tm

import sys
from datetime import datetime    
import logging

global g_triples 
g_triples = {}
global number_triple
number_triple = 0
global triples
triples = []
global duplicate
duplicate = ""
#global start_time
#start_time = 0
global user, password, port, host
user, password, port, host = "", "", "", ""
global join_table 
join_table = {}
global po_table
po_table = {}
global enrichment
enrichment = ""

global before_size_list
global after_size_list
global source_file_names
global exp_config
before_size_list=[]
after_size_list=[]
source_file_names=[]

dt_format='%d.%m.%Y %H:%M:%S,%f'
v_csv_output_path = "C:\\Users\\TorabinejadM\\Desktop\\Thesis\\implementation\\sdm-tib-github\\rml-normalizer\\experiments\\csv\\"
v_mapping_output_path="C:\\Users\\TorabinejadM\\Desktop\\Thesis\\implementation\\sdm-tib-github\\rml-normalizer\\experiments\\mappings\\"
v_main_dir=".//app//experiments//"

logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logging.basicConfig()
rootLogger = logging.getLogger()
rootLogger.setLevel(logging.INFO)

def log_calls(function_name,start_time,type=""):
    if type!="new_run":
        if start_time is None:
            rootLogger.info("")
            dt_now = datetime.now().strftime(dt_format)
            v_start_time=time.time()
            rootLogger.info("====================== "+function_name+" started at "+dt_now+"======================")
            return v_start_time
        else:
            dt_now = datetime.now().strftime(dt_format)
            v_elpased_time=time.time()-start_time
            rootLogger.info("execuion time: {} seconds".format(v_elpased_time))
            rootLogger.info("====================== "+function_name+" finished at "+dt_now+"======================")
            rootLogger.info("")
            return v_elpased_time
    else:
        if start_time is None:
            rootLogger.info("")
            rootLogger.info("")
            rootLogger.info("")
            dt_now = datetime.now().strftime(dt_format)
            v_start_time=time.time()
            rootLogger.info("%%%%%%%%%%%%%%%%%% "+function_name+" started at "+dt_now+" %%%%%%%%%%%%%%%%%%")
            return v_start_time
        else:
            dt_now = datetime.now().strftime(dt_format)
            v_elpased_time=time.time()-start_time
            rootLogger.info("execuion time: {} seconds".format(v_elpased_time))
            rootLogger.info("%%%%%%%%%%%%%%%%%% "+function_name+" finished at "+dt_now+" %%%%%%%%%%%%%%%%%%")
            return v_elpased_time

def log_error(function_name,error_msg):
    rootLogger.setLevel(logging.ERROR)
    rootLogger.error("Error occured in "+function_name)
    rootLogger.error("Error !!!!!!!!!")
    rootLogger.error(error_msg)
    rootLogger.error("Error !!!!!!!!!")
    rootLogger.setLevel(logging.INFO)
    rootLogger.info("!!!!!!!!!!!!!!!!!!!!! "+function_name+" failed !!!!!!!!!!!!!!!!!!!!!")

def mapping_parser(mapping_file):
    mapping_graph = rdflib.Graph()

    try:
        mapping_graph.load(mapping_file, format='n3')
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
    try:
        start_time=None
        start_time=log_calls("fd_parser",start_time)
        mapping_graph = rdflib.Graph()
    
        try:
            mapping_graph.load(mapping_file, format='n3')
        except Exception as n3_mapping_parse_exception:
            log_error('fd_parser',n3_mapping_parse_exception)
            log_error('fd_parser','Could not parse {} as a mapping file'.format(mapping_file))
            log_error('fd_parser',"Aborting...")
            sys.exit(1)
            
        fd_query="""
            prefix fd: <http://example-fd-set.com/>
            SELECT DISTINCT ?attr_name ?_attr ?_dep
            WHERE {
                ?triples_map_id fd:key ?_key.
                ?_key fd:column_name ?pkey.
                OPTIONAL {?_key fd:determine ?_attr.
                          ?_attr fd:column_name ?attr_name.
                OPTIONAL {?_attr fd:dependant ?_dep}
                }
                }"""
        
        pk_query="""
            prefix fd: <http://example-fd-set.com/>
            SELECT DISTINCT *
            WHERE {
                ?triples_map_id fd:key ?_key.
                ?_key fd:column_name ?pkey.
                }"""
        
        mapping_query_results = mapping_graph.query(fd_query)
        
        pk_query_results = mapping_graph.query(pk_query)
        
        transitive_list={}
        pkey=[]
        for result_triples_map in pk_query_results:
            pkey.append(str(result_triples_map.pkey).replace("{","").replace("}","").split(","))
        
        for result_triples_map in mapping_query_results:
            if str(result_triples_map.attr_name) in transitive_list:
                transitive_list[str(result_triples_map.attr_name)].append(str(result_triples_map._dep).replace("{","").replace("}","").split(","))
            else:
                transitive_list[str(result_triples_map.attr_name)]=[str(result_triples_map._dep).replace("{","").replace("}","").split(",")]
        log_calls("fd_parser",start_time)
        return transitive_list,pkey
    except Exception as e:
        log_error("fd_parser",e)
        return None
        
def normalize_rules(triples_map, fd_file, data_source, mapping_file,pk_list,dataset_index,dataset_name,rule_index):
    try:
        start_time=time.time()
        global before_size_list
        global after_size_list
        global exp_config
        
        object_list=[]
        object_list_on_sub=[]
        sub_is_pk=False
        
        exp_config["rule"+str(dataset_index*rule_index*2-1)]["dataset_name"]=""
        exp_config["rule"+str(dataset_index*rule_index*2)]["dataset_name"]=""
        
        new_total_size=0
        
        fd_list=fd_file
        new_rml_graph = Graph()
        old_rml_graph = Graph()
        
        base_uri=Namespace("http://tib.de/ontario/mapping#")
        rml=Namespace("http://semweb.mmlab.be/ns/rml#")
        ql= Namespace("http://semweb.mmlab.be/ns/ql#")
        r2rml= Namespace("http://www.w3.org/ns/r2rml#")
        ex= Namespace("http://example.com/")
        mapping_file_name=os.path.splitext((os.path.basename(mapping_file)))[0]+"--"+os.path.splitext((os.path.basename(dataset_name)))[0]
        new_rml_graph.parse(mapping_file, format='n3',publicID=base_uri)
        
        old_rml_graph.parse(mapping_file, format='n3',publicID=base_uri)
        
        
        base_obj=new_rml_graph.value(subject=None,predicate=rml['referenceFormulation'], object=ql['CSV'])
        base=new_rml_graph.value(subject=None,predicate=rml['logicalSource'], object=base_obj)
        new_rml_file = v_mapping_output_path+mapping_file_name+"_normal.ttl"
        
        old_rml_file = v_mapping_output_path+mapping_file_name+".ttl"
        
        deselected_cols=[]
        logial_source_node=new_rml_graph.value(subject=None,predicate=rml['referenceFormulation'], object=ql['CSV'])
        
        old_graph_logial_source=old_rml_graph.value(subject=None,predicate=rml['referenceFormulation'], object=ql['CSV'])
        
        
        subject_list=re.findall("{(.+?)}",triples_map.subject_map.value,re.DOTALL)
        
        new_rml_graph.namespace_manager.bind('rml', rml, override = True, replace=True)
        new_rml_graph.namespace_manager.bind('ql', ql, override = True, replace=True)
        new_rml_graph.namespace_manager.bind('rr', r2rml, override = True, replace=True)
        new_rml_graph.namespace_manager.bind('ex', ex, override = True, replace=True)
        
        child_mapping_file_name=mapping_file_name+"-"+"CT_"
        child_new_source_file = v_csv_output_path+"{}.csv"
        child_selected_cols=[]
        child_selected_cols.append(re.search("{(.+?)}",triples_map.subject_map.value).group(1))
        child_mapping_file_name=child_mapping_file_name+child_selected_cols[0]
        
        
        for pom_item in triples_map.predicate_object_maps_list:
            if pom_item.object_map.mapping_type == "template":
                object_value = re.search("{(.+?)}",pom_item.object_map.value).group(1)
            elif pom_item.object_map.mapping_type == "reference":
                object_value = pom_item.object_map.value
            object_list.append(object_value)
        
        for obj in object_list:
            fd_set=fd_list.get(str(obj))
            if fd_set:
                if set(fd_set[0])==set(subject_list) and len(fd_set)==1 and obj not in subject_list:
                    object_list_on_sub.append(obj)
        
        object_list_on_sub.extend(subject_list)
        for item in pk_list:
            if set(subject_list)==set(item):
                sub_is_pk=True
                break
        
        if object_list_on_sub and len(set(object_list_on_sub)-set(subject_list))!=len(object_list):
            set_rem_obj=set(object_list)-set(object_list_on_sub)
            if sub_is_pk:
                for obj in set_rem_obj:
                    common_rem_dep_obj=[]
                    fd_set=fd_list.get(obj)
                    
                    #Note: List of other existing left sides other than PK
                    rem_dep_set = [i for i in fd_set if i not in pk_list]
                    
                    #Note: Check if current object has partial dependency on subject through its referenced columns
                    for i in rem_dep_set:
                        common_rem_dep_obj=set(i).intersection(set(subject_list))
                        #Note: Consider the first macth (Todo: one can consider different parameters to match for example join selectivity)
                        if common_rem_dep_obj:
                            break
                        
                    #Note: Check if current object has partial/transitive dependency on subject through another object
                    if not common_rem_dep_obj:
                        for i in rem_dep_set:
                            common_rem_dep_obj=set(i).intersection(set(object_list_on_sub))
                            #Note: Consider the first macth (Todo: one can consider different parameters to match for example join selectivity)
                            if common_rem_dep_obj:
                                break
                    
                    if not common_rem_dep_obj:
                        print("No need to Normalize!!!!!!!!!!")
                    else:
                        join_condition=[]
                        selected_cols=[]
                        join_cols=""
                        join_condition=common_rem_dep_obj
                        selected_cols.append(obj)
                        for i in join_condition:
                            selected_cols.append(i)
                            join_cols=join_cols+"_"+str(i)
                        new_source_file = v_csv_output_path+mapping_file_name+"-PT{}.csv"
                        new_source_file = new_source_file.format(join_cols+"_"+str(obj))
                        
                        exp_config["rule"+str(dataset_index*rule_index*2-1)]["dataset_name"]=str(exp_config["rule"+str(dataset_index*rule_index*2-1)]["dataset_name"])+os.path.basename(new_source_file)+","
                        
                        df_new_source = pandas.read_csv(".//app//source//data//"+dataset_name,usecols=selected_cols)
                        df_new_source.drop_duplicates(keep = 'first', inplace = True)
                        df_new_source = df_new_source[selected_cols]
                        df_new_source.to_csv(v_main_dir+new_source_file, index=False)
                        
                        new_source = BNode()
                        new_subject = BNode()
                        new_join_condition=BNode()
                        
                        new_rml_graph.namespace_manager.bind('rml', rml, override = True, replace=True)
                        new_rml_graph.namespace_manager.bind('ql', ql, override = True, replace=True)
                        new_rml_graph.namespace_manager.bind('rr', r2rml, override = True, replace=True)
                        new_rml_graph.namespace_manager.bind('ex', ex, override = True, replace=True)
                        
                        new_triplesmap_uri = URIRef("#"+str(obj))
                        new_rml_graph.add( (new_triplesmap_uri, RDF.type,r2rml['TriplesMap'] ) )
                        new_rml_graph.add( (new_triplesmap_uri, rml['logicalSource'], new_source) )
                        new_rml_graph.add( (new_source, rml['source'], Literal(".//app//experiments//"+new_source_file)) )
                        new_rml_graph.add( (new_source, rml['referenceFormulation'], ql['CSV']) )
                        new_rml_graph.add( (new_triplesmap_uri, r2rml['subjectMap'], new_subject) )
                    
                        #to make the joins work for rocketrml, I need to ignore {}
                        new_subject_term=Literal("{"+obj+"}")
                    
                        #to make the joins work for rocketrml, I need to change template to reference
                        new_rml_graph.add( (new_subject, r2rml['template'], new_subject_term) )
                    
                        object_map_node=new_rml_graph.value(subject=None,predicate=rml['reference'], object=Literal(obj))                        
                        new_rml_graph.add( (object_map_node, r2rml['parentTriplesMap'], new_triplesmap_uri) )
                        for i in join_condition:    
                            new_rml_graph.add( (object_map_node, r2rml['joinCondition'], new_join_condition) )
                            new_rml_graph.add( (new_join_condition, r2rml['child'], Literal(i)) )
                            new_rml_graph.add( (new_join_condition, r2rml['parent'], Literal(i)) )
                        new_rml_graph.remove( (object_map_node, rml['reference'], Literal(obj)) )
                    
                        file_stats = os.stat(v_main_dir+new_source_file)
                        new_total_size=new_total_size+file_stats.st_size
            else:
                print("Not sub pk")
                set_decomposition=[]
                set_decompoisiton_non_prime=[]
                for item in pk_list:
                    print("check pks")
                    all_prime=False
                    all_mixed=False
                    all_non_prime=False
                    
                    #Note: Check if all other columns are prime. If all other columns are prime attributes of exactly one key then stop looking for other PKs.
                    if set_rem_obj.union(set(subject_list))==set(item) or set_rem_obj==set(item):
                        all_prime=True
                        set_decomposition=set_rem_obj
                        break
                    #Note: If the columns are prime attributes of different keys then look further for other PKs. (If in only one case all_prime is True then this will be ignored!!)
                    elif set(item).issubset(set_rem_obj.union(set(subject_list))):
                        all_mixed=True
                        set_decomposition.extend(i for i in item if i in set_rem_obj)
                        set_decompoisiton_non_prime=set_rem_obj-set(item)
                    else:
                        set_decompoisiton_non_prime=set_rem_obj-set(item)
                
                for obj in set_decomposition:
                    print(obj)
                    join_condition=[]
                    selected_cols=[]
                    join_cols=""
                    join_condition=subject_list
                    selected_cols.append(obj)
                    for i in join_condition:
                        selected_cols.append(i)
                        join_cols=join_cols+"_"+str(i)
                    new_source_file = v_csv_output_path+mapping_file_name+"-PT{}.csv"
                    new_source_file = new_source_file.format(join_cols+"_"+str(obj))
                    
                    exp_config["rule"+str(dataset_index*rule_index*2-1)]["dataset_name"]=str(exp_config["rule"+str(dataset_index*rule_index*2-1)]["dataset_name"])+os.path.basename(new_source_file)+","
                    
                    df_new_source = pandas.read_csv(".//app//source//data//"+dataset_name,usecols=selected_cols,dtype=str)
                    df_new_source.drop_duplicates(keep = 'first', inplace = True)
                    df_new_source = df_new_source[selected_cols]
                    df_new_source.to_csv(v_main_dir+new_source_file, index=False)
                    
                    new_source = BNode()
                    new_subject = BNode()
                    new_join_condition=BNode()
                    
                    new_rml_graph.namespace_manager.bind('rml', rml, override = True, replace=True)
                    new_rml_graph.namespace_manager.bind('ql', ql, override = True, replace=True)
                    new_rml_graph.namespace_manager.bind('rr', r2rml, override = True, replace=True)
                    new_rml_graph.namespace_manager.bind('ex', ex, override = True, replace=True)
                    
                    new_triplesmap_uri = URIRef("#"+str(obj))
                    new_rml_graph.add( (new_triplesmap_uri, RDF.type,r2rml['TriplesMap'] ) )
                    new_rml_graph.add( (new_triplesmap_uri, rml['logicalSource'], new_source) )
                    new_rml_graph.add( (new_source, rml['source'], Literal(".//app//experiments//"+new_source_file)) )
                    new_rml_graph.add( (new_source, rml['referenceFormulation'], ql['CSV']) )
                    new_rml_graph.add( (new_triplesmap_uri, r2rml['subjectMap'], new_subject) )
                    
                    #to make the joins work for rocketrml, I need to ignore {}
                    new_subject_term=Literal("{"+obj+"}")
                    
                    print(new_subject)
                    #to make the joins work for rocketrml, I need to change template to reference
                    new_rml_graph.add( (new_subject, r2rml['template'], new_subject_term) )
                    
                    object_map_node=new_rml_graph.value(subject=None,predicate=rml['reference'], object=Literal(obj))                        
                    print(obj)
                    new_rml_graph.add( (object_map_node, r2rml['parentTriplesMap'], new_triplesmap_uri) )
                    for i in join_condition:    
                        new_rml_graph.add( (object_map_node, r2rml['joinCondition'], new_join_condition) )
                        new_rml_graph.add( (new_join_condition, r2rml['child'], Literal(i)) )
                        new_rml_graph.add( (new_join_condition, r2rml['parent'], Literal(i)) )
                    new_rml_graph.remove( (object_map_node, rml['reference'], Literal(obj)) )
                    
                    file_stats = os.stat(v_main_dir+new_source_file)
                    new_total_size=new_total_size+file_stats.st_size
                    
                for obj in set_decompoisiton_non_prime:
                    common_rem_dep_obj=[]
                    fd_set=fd_list.get(obj)
                    
                    #Note: List of other existing left sides other than PKs and the Subject
                    rem_dep_set = [i for i in fd_set if i not in (pk_list and subject_list)]
                    
                    #Note: Check if current object has partial dependency on subject through its referenced columns
                    for i in rem_dep_set:
                        common_rem_dep_obj=set(i).intersection(set(subject_list))
                        #Note: Consider the first macth (Todo: one can consider different parameters to match for example join selectivity)
                        if common_rem_dep_obj:
                            break
                        
                    #Note: Check if current object has partial/transitive dependency on subject through another object
                    if not common_rem_dep_obj:
                        for i in rem_dep_set:
                            common_rem_dep_obj=set(i).intersection(set(object_list_on_sub))
                            #Note: Consider the first macth (Todo: one can consider different parameters to match for example join selectivity)
                            if common_rem_dep_obj:
                                break
                    
                    if not common_rem_dep_obj:
                        print("No need to Normalize!!!!!!!!!!")
                    else:
                        print("decompose a table with this obj and its fd")
                        join_condition=[]
                        selected_cols=[]
                        join_cols=""
                        join_condition=common_rem_dep_obj
                        selected_cols.append(obj)
                        print(selected_cols)
                        for i in join_condition:
                            selected_cols.append(i)
                            join_cols=join_cols+"_"+str(i)
                        new_source_file = v_csv_output_path+mapping_file_name+"-PT{}.csv"
                        new_source_file = new_source_file.format(join_cols+"_"+str(obj))
                        
                        exp_config["rule"+str(dataset_index*rule_index*2-1)]["dataset_name"]=str(exp_config["rule"+str(dataset_index*rule_index*2-1)]["dataset_name"])+os.path.basename(new_source_file)+","
                        
                        df_new_source = pandas.read_csv(".//app//source//data//"+dataset_name,usecols=selected_cols)
                        df_new_source.drop_duplicates(keep = 'first', inplace = True)
                        df_new_source = df_new_source[selected_cols]
                        df_new_source.to_csv(v_main_dir+new_source_file, index=False)
                        
                        new_source = BNode()
                        new_subject = BNode()
                        new_join_condition=BNode()
                        
                        new_rml_graph.namespace_manager.bind('rml', rml, override = True, replace=True)
                        new_rml_graph.namespace_manager.bind('ql', ql, override = True, replace=True)
                        new_rml_graph.namespace_manager.bind('rr', r2rml, override = True, replace=True)
                        new_rml_graph.namespace_manager.bind('ex', ex, override = True, replace=True)
                        
                        new_triplesmap_uri = URIRef("#"+str(obj))
                        new_rml_graph.add( (new_triplesmap_uri, RDF.type,r2rml['TriplesMap'] ) )
                        new_rml_graph.add( (new_triplesmap_uri, rml['logicalSource'], new_source) )
                        new_rml_graph.add( (new_source, rml['source'], Literal(".//app//experiments//"+new_source_file)) )
                        new_rml_graph.add( (new_source, rml['referenceFormulation'], ql['CSV']) )
                        new_rml_graph.add( (new_triplesmap_uri, r2rml['subjectMap'], new_subject) )
                    
                        #to make the joins work for rocketrml, I need to ignore {}
                        new_subject_term=Literal("{"+obj+"}")
                    
                        #to make the joins work for rocketrml, I need to change template to reference
                        new_rml_graph.add( (new_subject, r2rml['template'], new_subject_term) )
                    
                        object_map_node=new_rml_graph.value(subject=None,predicate=rml['reference'], object=Literal(obj))                        
                        new_rml_graph.add( (object_map_node, r2rml['parentTriplesMap'], new_triplesmap_uri) )
                        for i in join_condition:    
                            new_rml_graph.add( (object_map_node, r2rml['joinCondition'], new_join_condition) )
                            new_rml_graph.add( (new_join_condition, r2rml['child'], Literal(i)) )
                            new_rml_graph.add( (new_join_condition, r2rml['parent'], Literal(i)) )
                        new_rml_graph.remove( (object_map_node, rml['reference'], Literal(obj)) )
                    
                        file_stats = os.stat(v_main_dir+new_source_file)
                        new_total_size=new_total_size+file_stats.st_size
        

        elif object_list_on_sub and len(object_list_on_sub)==len(object_list):
            print("No need to normalize!!!")
        
        child_new_source_file=child_new_source_file.format(child_mapping_file_name)
        df_new_source = pandas.read_csv(".//app//source//data//"+dataset_name,usecols=object_list_on_sub)
        df_new_source.drop_duplicates(keep = 'first', inplace = True)
        df_new_source = df_new_source[object_list_on_sub]
        df_new_source.to_csv(v_main_dir+child_new_source_file, index=False)
        
        child_old_source_file=new_rml_graph.value(subject=logial_source_node,predicate=rml['source'], object=None)
        new_rml_graph.add( (logial_source_node, rml['source'], Literal(".//app//experiments//"+child_new_source_file)) )
        new_rml_graph.remove( (logial_source_node, rml['source'], Literal(child_old_source_file)) )
        new_rml_graph.serialize(destination=new_rml_file, format='n3',base="http://tib.de/ontario/mapping#")
        
        child_old_source_file=old_rml_graph.value(subject=old_graph_logial_source,predicate=rml['source'], object=None)
        
        shutil.copyfile(".//app//source//data//"+dataset_name, v_main_dir+v_csv_output_path+os.path.basename(child_old_source_file))
        
        shutil.copyfile(".//app//source//data//"+dataset_name, v_main_dir+v_csv_output_path+os.path.splitext((os.path.basename(old_rml_file)))[0]+'.csv')
        
        old_rml_graph.add( (old_graph_logial_source, rml['source'], Literal(".//app//experiments//"+v_csv_output_path+os.path.splitext((os.path.basename(old_rml_file)))[0]+".csv" )))
        old_rml_graph.remove( (old_graph_logial_source, rml['source'], Literal(child_old_source_file)) )
        old_rml_graph.serialize(destination=old_rml_file, format='n3',base="http://tib.de/ontario/mapping#")
        
        exp_config["rule"+str(dataset_index*rule_index*2-1)]["dataset_name"]=str(exp_config["rule"+str(dataset_index*rule_index*2-1)]["dataset_name"])+os.path.basename(child_new_source_file)
        
        exp_config["rule"+str(dataset_index*rule_index*2-1)]["filename"]=os.path.basename(new_rml_file)
        
        exp_config["rule"+str(dataset_index*rule_index*2)]["dataset_name"]=os.path.splitext((os.path.basename(old_rml_file)))[0]+".csv"
        
        exp_config["rule"+str(dataset_index*rule_index*2)]["filename"]=os.path.basename(old_rml_file)
        
    except Exception as e:
        log_error("normalize_rules",e)

def normalize(config_path="C:\\Users\\TorabinejadM\\Desktop\\Thesis\\implementation\\sdm-tib-github\\rml-normalizer\\source\\configfile.ini",csv_output_path="C:\\Users\\TorabinejadM\\Desktop\\Thesis\\implementation\\sdm-tib-github\\rml-normalizer\\experiments\\csv\\",mapping_output_path="C:\\Users\\TorabinejadM\\Desktop\\Thesis\\implementation\\sdm-tib-github\\rml-normalizer\\experiments\\mappings\\", main_dir=".//app//experiments//"):
    try:
        main_start_time=None
        main_start_time=log_calls("normalize",main_start_time,'new_run')
        global v_csv_output_path
        global v_mapping_output_path
        global source_file_names
        global exp_config
        mapping_file=""
        stat_data="\n"
        start_time=None
        dt_now=None
        
        v_csv_output_path=csv_output_path
        v_mapping_output_path=mapping_output_path
        config = ConfigParser(interpolation=ExtendedInterpolation())
        config.read(config_path)
        name_of_datasets=str(config["default"]["name_of_datasets"])
        number_of_rules=int(config["default"]["number_of_rules"])
        fd_file=str(config["default"]["fd"])
        list_datasets=name_of_datasets.split(",")
        
        default_stats_header="NAME,STARTED_AT,EXECUTION_TIME"
        try:
            with open(v_main_dir+"graph//normalizer_times.csv", 'r') as f_in:
                default_stats_header=f_in.readline()
        except FileNotFoundError as e:
            with open(v_main_dir+"graph//normalizer_times.csv", 'a+',newline="") as f_out:
                f_out.write(default_stats_header+"\n")
        
        exp_config = ConfigParser(interpolation=ExtendedInterpolation())
        exp_config.read(".//app//experiments//configfile.ini")
        exp_config["rules"]["number_of_rules"]=str(2*number_of_rules*len(list_datasets))

        for rule in range(0,number_of_rules):
            list_datasets= [ds_name.strip() for ds_name in name_of_datasets.split(",")]
            for dataset_number in range(0,len(list_datasets)):
                rule_i = "rule" + str(int(rule) + 1)
             
                mapping_file=config[rule_i]["mapping"]
                triples_map_list = mapping_parser(mapping_file)
            
                start_time=None
                start_time=log_calls("Normalization of dataset "+os.path.basename(list_datasets[dataset_number]),start_time)
                dt_now = datetime.now().strftime(dt_format)
            
                fd_list,pk_list = fd_parser(fd_file)
                for triples_map in triples_map_list:
                    normalize_rules(triples_map,fd_list,triples_map.data_source,mapping_file,pk_list,dataset_number+1,list_datasets[dataset_number],rule+1)
            
                elpased_time=log_calls("Normalization of dataset "+os.path.basename(list_datasets[dataset_number]),start_time)
                
            with open(v_main_dir+"graph//normalizer_times.csv", 'a+',newline="") as f_out:
                print(dt_now)
                f_out.write(str(os.path.splitext((os.path.basename(mapping_file)))[0])+",\""+str(dt_now)+"\","+str(elpased_time)+"\n")
                f_out.close()
        
            with open(".//app//experiments//configfile.ini", 'w') as configfile:
                exp_config.write(configfile)
                
        log_calls("normalize",main_start_time,'new_run')
    except Exception as e:
        log_error("normalize",e)
def main():
    config_path=".//app//source//configfile.ini"
    csv_output_path="csv//"
    mapping_output_path=".//app//experiments//mappings//"
    normalize(config_path,csv_output_path, mapping_output_path)

if __name__ == "__main__":
    main()
