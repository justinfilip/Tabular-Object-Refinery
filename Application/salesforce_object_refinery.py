import csv
import re
import os
import platform
from datetime import datetime
import xml.etree.ElementTree as ET

# Initialization:
# Read the parameters.csv file to get the selected method, directories / objects to process, fields to
# execute the process on, fields to return, and the search criteria.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# get the path of the current directory where this file was executed and 'go up one level' to the application root directory.



#             # 2.0 Begin processing batch matching string or regex against selected target fields

#             # Memory
#             # In the original version, memory is saved by processing the input dataset line by line, the rows that match your criteria will be saved to memory so the amount of memory used will vary depending on what your criteria is and the degree of correlation to your dataset
            
#             # CPU
#             # More faster = better, but it's decently quick anyway


#             # Replace with a function call
                
#                 # When the match is detected, instead of just returning the matching row to the output dataset or returning items that do not match the selected criteria, apply some transofrmation to the data
                    
#                     # Options

#                         # Method 02: 
#                         # Replace with content of the alteration field in the paramters file
                            
#                         # Method 03:
#                         # Alter using matching field sibling 
#                             # meaning: 

#                                 # > you're processing a row
#                                 # > your keyword or expression matches the user selected field in the dataset that is being searched within the row you are currently processing

#                                 # "if you find this in the dataset, use the content of the 'modifier' parameter from the parameters file as the keyword or part of an expression to 'dynamically transform the dataset'"


nav = ""

if platform.system() == "Windows":
    nav = "\\"
else:
    nav = "/"

current_path = str(os.getcwd())[0:-11]

def salesforce_object_refinery_main():
    with open(current_path + 'Application' + nav + 'parameters.csv', 'r', encoding='UTF-8') as parameters:
        param_reader = csv.reader(parameters, dialect='excel', skipinitialspace=True, delimiter=',', quotechar='"')
        param_field_selections = {}

        for param_index, param_field in enumerate(param_reader):
            object_parameters = []

            if param_index == 0:
                param_field_selections.update({param_index: param_field})
                continue

            else:
                # convert the selected objects in the parameters record (values) from the dictionary to a list
                object_parameters = param_field[2].split("#")

                for object_string_index, object_string in enumerate(object_parameters):
                    object_parameters[object_string_index] = object_string.strip()

            # 0.1 Get a list of files in the directory defined in the current batch of the parameters file (customer), create a second list containing the file names that end in .csv with the file extention removed (objects) and a dictionary containing an index and filename.
            files = os.listdir(current_path + 'Objects' + nav + str(param_field[6]) + nav)
            file_list = []
            objects = []
            file_objects = {}

            for file in files:
                if file.endswith(".csv") or file.endswith(".xml") or file.endswith(".tsv") or file.endswith(".txt"):
                    file_list.append(file)
                    objects.append(file[-4])
                else:
                    continue

            for object_index, object_item in enumerate(file_list):
                file_objects.update({object_index: object_item})

            # 1.0 Collect target objects, fields, search string, and desired return parameters from the parameters file.

            # populate the dictionary (target_objects) to store objects and indicies where the indicies of the available objects matched the indicies of the objects in the parameters record (batch)
            target_objects = {}

            for o, ob in enumerate(object_parameters):
                target_objects.update({o: file_objects[int(ob)]})

            # populate the dictionary (return_parameters) to store fields and indicies where the indicies of the available field matched the indicies of the fields in the parameters record (batch)
            return_parameters_field = param_field[4].split("|")
            return_parameters = {}

            for rp_index, rp_value in enumerate(return_parameters_field):
                return_parameters[rp_index] = rp_value.strip()

            # get a list of selected target fields within the selected object to execute the search on from the parameters record (batch)
            field_parameters_field = param_field[3].split("|")
            field_parameters = {}

            for fp_index, fp_value in enumerate(field_parameters_field):
                field_parameters[fp_index] = fp_value.strip()

            try:
                os.mkdir(current_path + 'Objects' + nav + 'Refined Objects' + nav + str(param_field[6]))
            except:
                pass



    # Method 0:
    # Match regular expression against the selected field and return positive results.
    # Apply a transformation to the target field if there is one present in the alteration field of the parameters file

            if int(param_field[1]) == 0:
                # 2.0 Begin processing batch matching string or regex against selected target fields

                # for each index, object in target_objects
                for to_index, to_object in target_objects.items():
                    if to_object.endswith(".csv"):

                        with open(current_path + 'Objects' + nav + str(param_field[6]) + nav + str(to_object), 'r', encoding='UTF-8') as engaged_object, open(current_path + 'Objects' + nav + 'Refined Objects' + nav + str(param_field[6]) + nav + str(datetime.now()) + '_' + str(target_objects[to_index]) + '.csv', "w+", encoding='UTF-8') as results:
                            filtered_object = (line.replace(
                                '\n' or '\r', '') for line in engaged_object)
                            object_reader = csv.reader(filtered_object, dialect='excel', skipinitialspace=True, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
                            results_writer = csv.writer(results, dialect='excel', skipinitialspace=True, delimiter=',', quotechar='"')
                            row_data = {}

                            # if to_object.endswith(".csv"):
                            for n, row in enumerate(engaged_object):
                                if n > 0:
                                    row_fields = list(next(object_reader))
                                    for rf_index, rf_content in enumerate(row_fields):
                                        rf_content_stripped = re.split(r'\W|\d', rf_content)
                                        rf_content_strung = ' '.join(rf_content_stripped)
                                        rf_content_remnlc = rf_content_strung.replace('(\n|,)', '')
                                        rf_content_cleaned = rf_content_remnlc.replace('  ', ' ')
                                        row_data.update({rf_index: rf_content})

                                        for entity, field_parameter in field_parameters.items():
                                            if (int(rf_index) == int(field_parameter)):
                                                # if the selected field contains the search criteria from the parameters file
                                                if re.match('.*' + param_field[5] + '.*', rf_content_cleaned):
                                                    result_list = []

                                                    # # #
                                                    # write the selected return field to the search results file
                                                    for x, rp in return_parameters.items():
                                                        if rp == field_parameter:

                                                            alteration = param_field[7]

                                                            if (len(alteration) < 1):
                                                                result_list.append(row_data[rf_index])

                                                            else:
                                                                result_list.append(alteration)

                                                        else:
                                                            try:
                                                                result_list.append(row_fields[int(rp)])
                                                            except:
                                                                continue

                                                    results_writer.writerow(result_list)
                                                else:
                                                    pass
                                            else:
                                                continue
                                else:
                                    row_fields = row.rstrip().split(',')
                                    first_row = []

                                    for ri, r in return_parameters.items():
                                        field_name = str(row_fields[int(r)]).replace('"', '')
                                        first_row.append(field_name)

                                    results_writer.writerow(first_row)

                    elif to_object.endswith(".xml"):
                        # parse xml and assign indecies to fields
                        with open(current_path + 'Refined Objects' + nav + str(param_field[6]) + '_' + str(target_objects[to_index]) + '_' + str(datetime.now()) + '.csv', "w+", encoding='UTF-8') as results:
                            results_writer = csv.writer(results, dialect='excel', skipinitialspace=True, delimiter=',', quotechar='"')

                            tree = ET.parse(current_path + 'Objects' + nav + str(param_field[6]) + nav + str(to_object))
                            root = tree.getroot()

                            #get the structure and write it as the first row of the results file
                            run_timer = 0
                            field_list = []
                            while run_timer < 1:
                                for count in range(len(root)):
                                    if run_timer < 1:
                                        for i, item in enumerate(root[count]):
                                            if i < len(root[count]):
                                                field_list.append(item.tag[48:len(item.tag)])
                                            else:
                                                pass     
                                    else:
                                        pass
                                    run_timer = run_timer + 1
                            results_writer.writerow(field_list)

                            #process every node of the tree and return results where the search critera matches the selected field
                            for count in range(len(root)):
                                field_item_list = []
                                for i, item in enumerate(root[count]):
                                    if i < len(root[count]):
                                        field_item_list.append(item.text)
                                    else:
                                        pass  
                                if re.match('.*' + param_field[5] + '.*', str(field_item_list[int(param_field[3])])):   

                                    alteration = param_field[7]

                                    if (len(alteration) < 1):
                                        results_writer.writerow(field_item_list)

                                    else:
                                        
                                        field_item_list[int(param_field[3])] = alteration
                                        results_writer.writerow(field_item_list)
                                else:
                                    pass
                    else:
                        pass

    # Method 1:
    # Match regular expression against the selected field and return negative results.
    # Apply a transformation to the target field if there is one present in the alteration field of the parameters file

            if int(param_field[1]) == 1:
                # 2.0 Begin processing batch matching string or regex against selected target fields
                # for each index, object in target_objects
                for to_index, to_object in target_objects.items():

                    ############
                    if to_object.endswith(".csv"):
                        # with open(current_path + 'Objects' + nav + str(param_field[6]) + nav + str(to_object), 'r', encoding='UTF-8') as engaged_object, open(current_path + 'Refined Objects' + nav + str(param_field[6]) + nav + str(target_objects[to_index]) + '_' + str(datetime.now()) + '.csv', "w+", encoding='UTF-8') as results:

                        with open(current_path + 'Objects' + nav + str(param_field[6]) + nav + str(to_object), 'r', encoding='UTF-8') as engaged_object, open(current_path + 'Objects' + nav + 'Refined Objects' + nav + str(param_field[6]) + nav + str(datetime.now()) + '_' + str(target_objects[to_index]) + '.csv', "w+", encoding='UTF-8') as results:


                            filtered_object = (line.replace('\n' or '\r', '') for line in engaged_object)
                            object_reader = csv.reader(filtered_object, dialect='excel', skipinitialspace=True, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
                            results_writer = csv.writer(results, dialect='excel', skipinitialspace=True, delimiter=',', quotechar='"')

                            
                            row_data = {}

                            # if to_object.endswith(".csv"):
                            for n, row in enumerate(engaged_object):
                                if n > 0:
                                    row_fields = list(next(object_reader))

                                    for rf_index, rf_content in enumerate(row_fields):
                                        rf_content_stripped = re.split(r'\W|\d', rf_content)
                                        rf_content_strung = ' '.join(rf_content_stripped)
                                        rf_content_remnlc = rf_content_strung.replace('(\n|,)', '')
                                        rf_content_cleaned = rf_content_remnlc.replace('  ', ' ')
                                        row_data.update({rf_index: rf_content_cleaned})

                                        for entity, field_parameter in field_parameters.items():
                                            if (int(rf_index) == int(field_parameter)):
                                                # if the selected field contains the search criteria from the parameters file
                                                if re.match('.*' + param_field[5] + '.*', rf_content_cleaned):
                                                    pass

                                                else:
                                                    result_list = []

                                                    # write the selected return field to the search results file
                                                    for x, rp in return_parameters.items():
                                                        if rp == field_parameter:

                                                            alteration = param_field[7]

                                                            if (len(alteration) < 1):
                                                                result_list.append(row_data[rf_index])

                                                            else:
                                                                result_list.append(alteration)

                                                            # result_list.append(row_data[rf_index])

                                                        else:
                                                            try:
                                                                result_list.append(row_fields[int(rp)])
                                                            except Exception as e:
                                                                continue

                                                    results_writer.writerow(result_list)
                                            else:
                                                continue
                                else:
                                    row_fields = row.rstrip().split(',')
                                    first_row = []

                                    for ri, r in return_parameters.items():
                                        field_name = str(row_fields[int(r)]).replace('"', '')
                                        first_row.append(field_name)

                                    results_writer.writerow(first_row)

                    elif to_object.endswith(".xml"):
                        # parse xml and assign indecies to fields
                        with open(current_path + 'Refined Objects' + nav + str(param_field[6]) + '_' + str(target_objects[to_index]) + '_' + str(datetime.now()) + '.csv', "w+", encoding='UTF-8') as results:
                            results_writer = csv.writer(results, dialect='excel', skipinitialspace=True, delimiter=',', quotechar='"')
                            tree = ET.parse(current_path + 'Objects' + nav + str(param_field[6]) + nav + str(to_object))
                            root = tree.getroot()

                            #get the structure and write it as the first row of the results file
                            run_timer = 0
                            field_list = []
                            while run_timer < 1:
                                for count in range(len(root)):
                                    if run_timer < 1:
                                        for i, item in enumerate(root[count]):
                                            if i < len(root[count]):
                                                field_list.append(item.tag[48:len(item.tag)])
                                            else:
                                                pass     
                                    else:
                                        pass
                                    run_timer = run_timer + 1

                            results_writer.writerow(field_list)

                            #process every node of the tree and return results where the search critera matches the selected field
                            for count in range(len(root)):
                                field_item_list = []
                                for i, item in enumerate(root[count]):
                                    if i < len(root[count]):
                                        field_item_list.append(item.text)
                                    else:
                                        pass  
                                if re.match('.*' + param_field[5] + '.*', str(field_item_list[int(param_field[3])])):   
                                    pass
                                else:

                                    alteration = param_field[7]

                                    if (len(alteration) < 1):
                                        results_writer.writerow(field_item_list)

                                    else:
                                        
                                        field_item_list[int(param_field[3])] = alteration
                                        results_writer.writerow(field_item_list)
                                    # results_writer.writerow(field_item_list)

                    elif to_object.endswith(('.tsv', '.txt')):
                        with open(current_path + 'Objects' + nav + str(param_field[6]) + nav + str(to_object), 'r', encoding='UTF-8') as engaged_object, open(current_path + 'Refined Objects' + nav + str(param_field[6]) + '_' + str(target_objects[to_index]) + '_' + str(datetime.now()) + '.csv', "w+", encoding='UTF-8') as results:
                            object_reader = csv.reader(engaged_object, delimiter='\t')
                            results_writer = csv.writer(results, dialect='excel', skipinitialspace=True, delimiter=',', quotechar='"')

                            for row in object_reader:
                                row_data = {}

                                for f_num, field in enumerate(row):
                                    row_data.update({f_num: field})
                                    result_list = []
                                    
                                    for entity, field_parameter in field_parameters.items():
                                        if (f_num == int(field_parameter)):
                                            if re.match('.*' + param_field[5] + '.*', field):
                                                continue
                                            else:
                                                try:
                                                    # results_writer.writerow(row)

                                                    alteration = param_field[7]
                                                    
                                                    if (len(alteration) < 1):
                                                        results_writer.writerow(row)

                                                    else:
                                                        
                                                        row[int(param_field[3])] = alteration
                                                        results_writer.writerow(row)




                                                except Exception as e:
                                                    continue
                                        else:
                                            continue
                    else:
                        pass
            else:
                pass


    # Method 2:
    # Match regular expression against the selected field and return negative results.
            if int(param_field[1]) == 2:



                # xtrachz
                # 2.0 Begin processing batch matching string or regex against selected target fields





                # for each index, object in target_objects
                for to_index, to_object in target_objects.items():
                    if to_object.endswith(".csv"):
                        with open(current_path + 'Objects' + nav + str(param_field[6]) + nav + str(to_object), 'r', encoding='UTF-8') as engaged_object, open(current_path + 'Refined Objects' + nav + str(param_field[6]) + '_' + str(target_objects[to_index]) + '_' + str(datetime.now()) + '.csv', "w+", encoding='UTF-8') as results:
                            filtered_object = (line.replace('\n' or '\r', '') for line in engaged_object)
                            object_reader = csv.reader(filtered_object, dialect='excel', skipinitialspace=True, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
                            results_writer = csv.writer(results, dialect='excel', skipinitialspace=True, delimiter=',', quotechar='"')
                            row_data = {}

                            # if to_object.endswith(".csv"):
                            for n, row in enumerate(engaged_object):
                                if n > 0:
                                    row_fields = list(next(object_reader))

                                    for rf_index, rf_content in enumerate(row_fields):
                                        rf_content_stripped = re.split(r'\W|\d', rf_content)
                                        rf_content_strung = ' '.join(rf_content_stripped)
                                        rf_content_remnlc = rf_content_strung.replace('(\n|,)', '')
                                        rf_content_cleaned = rf_content_remnlc.replace('  ', ' ')
                                        row_data.update({rf_index: rf_content_cleaned})

                                        for entity, field_parameter in field_parameters.items():
                                            if (int(rf_index) == int(field_parameter)):
                                                # if the selected field contains the search criteria from the parameters file
                                                if re.match('.*' + param_field[5] + '.*', rf_content_cleaned):
                                                    pass

                                                else:
                                                    result_list = []

                                                    # write the selected return field to the search results file
                                                    for x, rp in return_parameters.items():
                                                        if rp == field_parameter:
                                                            # result_list.append(row_data[rf_index])

                                                            alteration = param_field[7]

                                                            if (len(alteration) < 1):
                                                                result_list.append(row_data[rf_index])

                                                            else:
                                                                result_list.append(alteration)

                                                        else:
                                                            try:
                                                                result_list.append(row_fields[int(rp)])
                                                            except Exception as e:
                                                                continue

                                                    results_writer.writerow(result_list)
                                            else:
                                                continue
                                else:
                                    row_fields = row.rstrip().split(',')
                                    first_row = []

                                    for ri, r in return_parameters.items():
                                        field_name = str(row_fields[int(r)]).replace('"', '')
                                        first_row.append(field_name)

                                    results_writer.writerow(first_row)

                    elif to_object.endswith(".xml"):
                        # parse xml and assign indecies to fields
                        with open(current_path + 'Refined Objects' + nav + str(param_field[6]) + '_' + str(target_objects[to_index]) + '_' + str(datetime.now()) + '.csv', "w+", encoding='UTF-8') as results:
                            results_writer = csv.writer(results, dialect='excel', skipinitialspace=True, delimiter=',', quotechar='"')
                            tree = ET.parse(current_path + 'Objects' + nav + str(param_field[6]) + nav + str(to_object))
                            root = tree.getroot()

                            #get the structure and write it as the first row of the results file
                            run_timer = 0
                            field_list = []
                            while run_timer < 1:
                                for count in range(len(root)):
                                    if run_timer < 1:
                                        for i, item in enumerate(root[count]):
                                            if i < len(root[count]):
                                                field_list.append(item.tag[48:len(item.tag)])
                                            else:
                                                pass     
                                    else:
                                        pass
                                    run_timer = run_timer + 1

                            results_writer.writerow(field_list)

                            #process every node of the tree and return results where the search critera matches the selected field
                            for count in range(len(root)):
                                field_item_list = []
                                for i, item in enumerate(root[count]):
                                    if i < len(root[count]):
                                        field_item_list.append(item.text)
                                    else:
                                        pass  
                                if re.match('.*' + param_field[5] + '.*', str(field_item_list[int(param_field[3])])):   
                                    pass
                                else:
                                    
                                    alteration = param_field[7]

                                    if (len(alteration) < 1):
                                        results_writer.writerow(field_item_list)

                                    else:
                                        # result_list.append(alteration)
                                        field_item_list[int(param_field[3])] = alteration
                                        results_writer.writerow(field_item_list)

                    elif to_object.endswith(('.tsv', '.txt')):
                        with open(current_path + 'Objects' + nav + str(param_field[6]) + nav + str(to_object), 'r', encoding='UTF-8') as engaged_object, open(current_path + 'Refined Objects' + nav + str(param_field[6]) + '_' + str(target_objects[to_index]) + '_' + str(datetime.now()) + '.csv', "w+", encoding='UTF-8') as results:
                            object_reader = csv.reader(engaged_object, delimiter='\t')
                            results_writer = csv.writer(results, dialect='excel', skipinitialspace=True, delimiter=',', quotechar='"')

                            for n, row in enumerate(engaged_object):
                                if n > 0:
                                    row_fields = list(next(object_reader))

                                    for rf_index, rf_content in enumerate(row_fields):
                                        rf_content_stripped = re.split(r'\W|\d', rf_content)
                                        rf_content_strung = ' '.join(rf_content_stripped)
                                        rf_content_remnlc = rf_content_strung.replace('(\n|,)', '')
                                        rf_content_cleaned = rf_content_remnlc.replace('  ', ' ')
                                        row_data.update({rf_index: rf_content_cleaned})

                                        for entity, field_parameter in field_parameters.items():
                                            if (int(rf_index) == int(field_parameter)):
                                                # if the selected field contains the search criteria from the parameters file
                                                if re.match('.*' + param_field[5] + '.*', rf_content_cleaned):
                                                    pass

                                                else:
                                                    result_list = []

                                                    # write the selected return field to the search results file
                                                    for x, rp in return_parameters.items():
                                                        if rp == field_parameter:
                                                            # result_list.append(row_data[rf_index])

                                                            alteration = param_field[7]

                                                            if (len(alteration) < 1):
                                                                result_list.append(row_data[rf_index])

                                                            else:
                                                                result_list.append(alteration)

                                                        else:
                                                            try:
                                                                result_list.append(row_fields[int(rp)])
                                                            except Exception as e:
                                                                continue

                                                    results_writer.writerow(result_list)
                                            else:
                                                continue
                                else:
                                    row_fields = row.rstrip().split('\t')
                                    first_row = []

                                    for ri, r in return_parameters.items():
                                        field_name = str(row_fields[int(r)]).replace('"', '')
                                        first_row.append(field_name)

                                    results_writer.writerow(first_row)
                    else:
                        pass
            else:
                pass

# Execute the program. Program will hault once the last parameters record is processed.
salesforce_object_refinery_main()
