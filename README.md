# Tabular Object Refinery

Use plain text, regular expressions, and an inclusionary or exclusionary selection method to refine tabular data sets (Salesforce Objects {.xml} or Comma Separated Values{.csv, .txt, .tsv}) into a smaller, easy to analyze CSV file(s) to discover trends, patterns, and more in your data.</br></br>

Regular Expression support allows for deep contextual analysis of your customer or enterprise data. You can create a structural linquistic algorithm to focus in on a common speech trend for a situation, or simply look for exact phrases that indicate an interesting data point.</br></br>

Every row in your data set is processed sequentially. By not loading the entire data set into memory, this tool should work on the most number of various power computers. If your criteria matches the record being processed, it is either included or excluded in the result data based on your filter mode selection.

---

## How to use:

### Step 1 

Place your tabular data into a new folder within the 'Objects' directory. You can name the folder whatever you'd like, but it is useful to name it according to your use case or question that you're answering, as well as the date in a non-complicated format.

### Step 2

Build the Customer Object Field Index (COFI) file by executing 'Application/build_cofi.py'. Use the resulting output file, 'Objects/cofi.csv', to understand the available fields and their indicies from your data set. Use these to build your 'Application/parameters.csv' file, as described here:

Building your parameters file:

From left to right, the fields required in the parameters file are:

#### Batch ID
    This can be whatever you'd like, it is ignored.

#### Selected Method (0 or 1)
    
    Include ( parameter value: 0 )
    For each record in the data structure, check the user-defined ‘field to search’ for content matching the user-defined String or Regular Expression, known as the ‘criteria’. For each record that matches, the user-defined ‘fields to return’ for the record are written to the result file. 
    
    Exclude ( parameter value: 1 )
    For each record in the data structure, check the user-defined ‘field to search’ for content matching the user-defined String or Regular Expression, known as the ‘criteria’. For each record that matches, skip the record. For each record that does not match the ‘criteria’, the user-defined ‘fields to return’ for the record will be written to the result file. 

#### Selected Objects
    Integer representing the file present within the target 'Objects/{Use Case Name}' directory. You may process multiple 'chunks' of a data set or multiple data sets that share the same structure at once by including multiple object values, separated by a hashtag '#'. (E.g. 0#1#2)

#### Selected Field
    Integer representing the desired field to match against your plain text or Regular Expression. Only one value is supported.

#### Return Fields
    Pipe delimited integers representing the fields from the original data set that you would like to either return or exclude from the processed data set, depending on the selected method. (E.g. 2|3|4)

#### Search Criteria
    Your plain text or Regular Expression that you would like to use to refine the data set.

#### Customer
    The name of the directory within the Objects directory that contains the data sets for this job.

### Step 3
Execute Application/salesforce_object_refinery.py.

### Step 4
Locate and utilize the result data sets in 'Objects/Refined Objects/{Use Case Name}'.

---

Supported Data Structures:
XML
CSV
TSV
TXT

Result Data Structure:
CSV

Requirements:
Python (2.7 or 3+)

Supported Platforms:
Windows
Mac OS
Linux
