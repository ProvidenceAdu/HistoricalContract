# -*- coding: utf-8 -*-

import arcpy
import pandas as pd
import numpy as np
import os
from datetime import datetime
import time
import sys


class Toolbox:
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Historical Contract GCP"
        self.alias = "HistoricalContractGCP"

        # List of tool classes associated with this toolbox
        self.tools = [HistoricalContract]


class HistoricalContract:
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Historical Contracts"
        self.description = "Historical Contracts GCP"
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define the tool parameters."""

        inputExcelTable = arcpy.Parameter( displayName="Select Input Excel Sheet (Skip if Reverifying Records)",  
                             name="input_excel",
                             datatype="DEFile",
                             parameterType="Optional",
                             direction="Input")
        inputExcelTable.description = "Select the table that contains the data for the processing tool."
        inputExcelTable.filter.list = ["xls", "xlsx"]

        inputSDE = arcpy.Parameter( displayName="Reverify Records",  
                             name="input_table",
                             datatype="DETable",
                             parameterType="Optional",
                             direction="Input")
        inputSDE.value = "C:\\Users\\adup1373\Documents\\Historical Contract\\HistoricalContract.gdb\\SDEEvents"
        inputSDE.description = "Select the SDE table that contains the data for points and line events. This table is also the target table for appending records"

        inputRouteFC = arcpy.Parameter(displayName="Input Route Feature Class",  #The route features on which events will be located.
                            name="route_fc",
                            datatype="DEFeatureClass",     
                            parameterType="Required",
                            direction="Input")
        inputRouteFC.value = "C:\\Users\\adup1373\\Documents\\Historical Contract\\HistoricalContract.gdb\\MilepostCalibratedRoutes"
        inputRouteFC.description = "Select the route feature class containing the routes to be used in the event generation."
        inputRouteFC.filter.list = ["Polyline"]

        
        routeIDField =  arcpy.Parameter(displayName="Route Identifier Field",    #The field containing values that uniquely identify each route
                              name="route_id_field",
                              datatype="Field",
                              parameterType="Required",
                              direction="Input")
        routeIDField.parameterDependencies = [inputRouteFC.name]                 #Set dependencies for Route ID field using the route feature class
        routeIDField.value = 'RouteID'                                           # Set default value for Route ID field to RouteId
        routeIDField.description = "Select the field in the Route Feature Class that uniquely identifies each route (e.g., RouteId)."
        
        pointEvent =  arcpy.Parameter(displayName="Scoping Contract Points", 
                                   name = 'Point_fc',
                                   datatype="DEFeatureClass",
                                   parameterType="Optional",
                                   direction="Input")
        pointEvent.value = "C:\\Users\\adup1373\\Documents\\Historical Contract\\HistoricalContract.gdb\\ScopingPointHistoricalContract"
        pointEvent.description =  "Select the Historical Contract Point Feature class to which the generated point events will be appended."
        pointEvent.filter.list = ["Point"]

        lineEvent =  arcpy.Parameter(displayName="Scoping Contract Line", 
                                name = 'Line_fc',
                                datatype="DEFeatureClass",
                                parameterType="Optional",
                                direction="Input")
        lineEvent.value = "C:\\Users\\adup1373\\Documents\\Historical Contract\\HistoricalContract.gdb\\ScopingLineHistoricalContract"
        lineEvent.description =  "Select the Historical Contract Line Feature class to which the generated line events will be appended."
        lineEvent.filter.list = ["Polyline"]
    
        
        advancedoptions = arcpy.Parameter(displayName="Show Extra Options", 
                                name = 'showoptions',
                                datatype="GPBoolean",
                                parameterType="Optional",
                                direction="Input")
        advancedoptions.description = "Check this option to show additional parameters."
                                         
        
        params = [inputExcelTable,inputSDE,inputRouteFC,routeIDField,pointEvent,lineEvent,advancedoptions]

        return params

    def isLicensed(self):
        """Set whether the tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        
        show = parameters[6].value

        parameters[2].enabled = show
        parameters[3].enabled = show
        parameters[4].enabled = show
        parameters[5].enabled = show

        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        start_time = time.time()

    
        # Add Outputs to map
        arcpy.env.addOutputsToMap = True

        # Overwrite outputs
        arcpy.env.overwriteOutput = True

        # Set parameters for the input

        inputExcel = parameters[0].valueAsText     # input Excel Table
        input_table = parameters[1].valueAsText    # input SDE Table 
        route_fc = parameters[2].valueAsText        #The route features on which events will be located.
        route_id_field = parameters[3].valueAsText  # The field containing values that uniquely identify each route 
        Point_fc = parameters[4].valueAsText  # Point Event Feature class to which events will be appended
        Line_fc = parameters[5].valueAsText  # Line Event Feature class to which events will be appended
        showadvance = parameters[6].valueAsText  # Check box for to show extra parameter options 
       

        newrecords = True   # Set all new records to true until otherwise

        if inputExcel:

             # Read Input Excel Table 

            inputexceldata = pd.read_excel(inputExcel)
            arcpy.AddMessage(f"The total number of records in this table is {inputexceldata.shape} and the columns include {inputexceldata.columns}")                      
    
            #  Check if verified column exist 

            if 'Verified' not in inputexceldata.columns:
                arcpy.AddWarning('Table has no column called verified')
                raise Exception ('Table is not a mastertable, input a mastertable with verified column')
            
            # Filter for verified records that are either 1 or 2
            
            verifiedinput = inputexceldata[inputexceldata['Verified'].isin([1,2])]
            arcpy.AddMessage(f"The total number of filtered records in this table is {verifiedinput.shape}")

            if verifiedinput.empty:   # check if table is not empty
                arcpy.AddWarning(f"No verified records found. Table has {len(verifiedinput)} records to process.")
                return 
            else: 
                # export verified records
                # arcpy.AddMessage(f"Exporting verified records as csv")
                finalverifiedrecords = os.path.join(arcpy.env.scratchFolder,'verifiedrecords.csv')
                verifiedinput.to_csv(finalverifiedrecords,index = False)

                # export verified records to a geodatabase 

                arcpy.conversion.ExportTable(finalverifiedrecords,'verifiedrecordsHC',"","NOT_USE_ALIAS","" ,None)

                # Get fields that are common in the verified records table and the sde table

                inputsdefields = {field.name for field in arcpy.ListFields(input_table)if field.type not in ("OID")}  
                inputexcelrecordsfields = {field.name for field in arcpy.ListFields('verifiedrecordsHC')if field.type not in ("OID")}
                
                fields =  list(inputsdefields & inputexcelrecordsfields)  # Get fields that are common in both tables

                # arcpy.AddMessage(f"These are the fields {fields} common in the SDE table and the Excel verification table")

                indexofEventId = fields.index('ID')  # Get the index of the Unique ID fields, which will be used to check for the duplicates 

                sderecordsID = {row[0] for row in arcpy.da.SearchCursor(input_table,['ID'])}  # Get all IDs in the tables that will be used to check for duplicates 
                # arcpy.AddMessage(sderecordsID)

                inital_count = int(arcpy.management.GetCount(input_table)[0])
        
                # Check for duplicates and update table

                with arcpy.da.InsertCursor(input_table,fields) as insertcursor:
                    with arcpy.da.SearchCursor("verifiedrecordsHC",fields) as searchcursor:
                        totalrowsupdated  = 0
                        for row in searchcursor:
                            if str(row[indexofEventId]) not in sderecordsID:
                                insertcursor.insertRow(row)
                                totalrowsupdated  += 1
                    arcpy.AddMessage(f"A total of {totalrowsupdated} verified records have been added to the SDE Table.")

                final_count = int(arcpy.management.GetCount(input_table)[0])

                difference = final_count - inital_count

                if arcpy.Exists("verifiedrecordsHC"):
                    arcpy.Delete_management("verifiedrecordsHC")  # Delete table 


                if difference > 0:
                    newrecords = True
                else:
                    newrecords = False
    
        
        if newrecords:

            # Make a temporary table from input table

            arcpy.MakeTableView_management(input_table, "verifiedrecords")

            inputrecordscount = int(arcpy.management.GetCount("verifiedrecords")[0])
            arcpy.AddMessage(f"There are: {inputrecordscount} records in the input table")

            # Add Field FROM_MEASURE and TO_MEASURE Fields 

            if 'FROM_MEASURE' not in [f.name for f in arcpy.ListFields("verifiedrecords")]: 
                arcpy.AddField_management("verifiedrecords", 'FROM_MEASURE', "DOUBLE",100,50,None,'FROM_MEASURE')
            arcpy.CalculateField_management("verifiedrecords", 'FROM_MEASURE',  "None if not str(!MilepostBegin!).strip().replace('.', '', 1).isdigit() else float(!MilepostBegin!)", "PYTHON3")

            if 'TO_MEASURE' not in [f.name for f in arcpy.ListFields("verifiedrecords")]: 
                
                arcpy.AddField_management("verifiedrecords", 'TO_MEASURE', "DOUBLE",100,50,None,'TO_MEASURE')
            arcpy.CalculateField_management("verifiedrecords", 'TO_MEASURE',  "None if not str(!MilepostEnd!).strip().replace('.', '', 1).isdigit() else float(!MilepostEnd!)", "PYTHON3")
            
            

            # Set condition for verified and reverified records then filter table for INVERSE of the condition 

            arcpy.management.SelectLayerByAttribute("verifiedrecords","NEW_SELECTION","(Verified = 1) AND (Reverified IS NULL Or Reverified = 1)",None)

            hasunverified = any(arcpy.da.SearchCursor("verifiedrecords", ["OID@"]))
            Totalunverified = arcpy.management.GetCount('verifiedrecords')
            arcpy.AddMessage(f"There are: {Totalunverified[0]} verified records")
            
            # Set Reverify to 0 when condition is true
            if hasunverified:
            

                arcpy.management.SelectLayerByAttribute("verifiedrecords", "CLEAR_SELECTION")


            # Set condition for verified and reverified records then filter table 

                arcpy.MakeTableView_management(input_table, "verifiedrecords","Verified = 1 And (Reverified IS NULL Or Reverified = 1) And (LOWER(MilepostEnd) = 'p' Or MilepostEnd = MilepostBegin)")
                

                totalpointeventrecords = arcpy.management.GetCount('verifiedrecords')
                arcpy.AddMessage(f"There are: {totalpointeventrecords[0]} point event records to be processed")
                
                
                # Check if table is a point event table 

                isPointEvent = False 
                with arcpy.da.SearchCursor('verifiedrecords',['OID@'])  as cursor: 

                    if next(cursor, None) is not None :
                                isPointEvent = True
                    else:
                        arcpy.AddMessage('There are no Point Events to Locate, Skipping to Line Events')

        #################### POINT EVENT ####################################
                # Make route event layer
                if isPointEvent:
                    arcpy.AddMessage(f"Starting Make Route Event Layer For The Point Events")

                    totalpointeventrecords = arcpy.management.GetCount('verifiedrecords')

                    eventidfields = ['RouteId','POINT','FROM_MEASURE']

                    in_event_properties = "; ".join(eventidfields)

                    arcpy.AddMessage(f"The input event properties are: {in_event_properties}")
                    arcpy.AddMessage(f"Route FC is  {route_fc}")
                    arcpy.AddMessage(f"Route ID Field is  {route_id_field}")
                    desc = arcpy.Describe('verifiedrecords')
                    arcpy.AddMessage(f"The input event table is {desc.name}")

                    arcpy.lr.MakeRouteEventLayer(route_fc,
                                                route_id_field,
                                                'verifiedrecords',
                                                "; ".join(eventidfields),
                                                'NDOTPointEvents',
                                                None,
                                                "ERROR_FIELD",
                                                "NO_ANGLE_FIELD",
                                                "NORMAL",
                                                "ANGLE",
                                                "LEFT",
                                                "POINT")
                    arcpy.AddMessage(f"Make Route Event Layer for Point Events has successfully processed")

                    # Filter for events located with error 
                    arcpy.AddMessage(f"Check for Point Events that had Location Error")

                    arcpy.MakeTableView_management('NDOTPointEvents',"LocationErrorPE","(Verified = 1) And (Reverified IS NULL Or Reverified = 1) And (LOWER(MilepostEnd) = 'p' OR MilepostEnd = MilepostBegin) And (LOC_ERROR <> 'NO ERROR')")

                    locerrorpoints = int(arcpy.management.GetCount('LocationErrorPE')[0])
                    arcpy.AddMessage(f"Number of records that had Location Error: {locerrorpoints}")

                    # Get contract IDs for point all events where there is Location error

                    contractIDs = {row[0] for row in arcpy.da.SearchCursor('LocationErrorPE',['ID'])}                 # get a set of all IDS

                
                    
                    if contractIDs:                                                                                             # check if it is a non-empty set
                        arcpy.AddMessage(f"Updating the reverified field in the input table for all records with Location Error to 0")  

                        locerrorcontractIDs = ", ".join([f"'{str(ids)}'" for ids in contractIDs])                               # Loop through all the ids, add quotation marks, and join them to an empty string.
                        contractIDsquery = f"ID IN ({locerrorcontractIDs})"

                        arcpy.MakeTableView_management(input_table, "pointverifiedrecords",contractIDsquery)

                       
                        locerrorIds = {row[0]: row[1] for row in arcpy.da.SearchCursor('LocationErrorPE', ['ID', 'LOC_ERROR'])}


                        with arcpy.da.UpdateCursor("pointverifiedrecords",['ID','Reverified','LocError']) as cursor:
                            totalrowsupdated  = 0
                            for row in cursor:
                                id = row[0]
                                if id in locerrorIds:
                                    row[1] = 0 
                                    row[2] = locerrorIds[id]
                                    cursor.updateRow(row)
                                    totalrowsupdated  += 1
                        desc = arcpy.Describe(input_table)
                        arcpy.AddMessage(f"A total of {totalrowsupdated} records had the reverified field set to in the input table {desc.name}.")


                        arcpy.management.Delete("pointverifiedrecords")

                    else:
                        arcpy.AddMessage(f"Since there were no records with Location Errors, No record had the reverified field updated 0")

                    arcpy.management.Delete("LocationErrorPE")

                    # Filter for events that have No Error 

                    arcpy.MakeTableView_management('NDOTPointEvents',"NOLocationErrorPE","(Verified = 1) And (Reverified IS NULL Or Reverified = 1) And (LOWER(MilepostEnd) = 'p' OR MilepostEnd = MilepostBegin) And (LOC_ERROR = 'NO ERROR')")

                    # Append point event data to historical contract table

                    with arcpy.da.SearchCursor("NOLocationErrorPE",['OID@']) as cursor:   # Check if Table is not empty
                        if next(cursor, None) is not None:
                            arcpy.AddMessage(f"Appending records that were located with no errors to Point Event feature class")

                            nolocerrorfields = {field.name for field in arcpy.ListFields("NOLocationErrorPE")if field.type not in ("OID")}  
                            pointeventfields = {field.name for field in arcpy.ListFields(Point_fc)if field.type not in ("OID")}
                            fields = ['SHAPE@'] + list(nolocerrorfields & pointeventfields) # Get fields that are common in both tables, and add geometry field

                            indexofEventId = fields.index('ID')  # Get the index of the contract number in the fields table (use this index in the search cursor)
                            recordstoappend = {row[0] for row in arcpy.da.SearchCursor(Point_fc,['ID'])}  #Get all Contract Numbers in the tables located with no error 
                        
                            with arcpy.da.InsertCursor(Point_fc,fields) as insertcursor:
                                with arcpy.da.SearchCursor("NOLocationErrorPE",fields) as searchcursor:
                                    for row in searchcursor:
                                        if str(row[indexofEventId]) not in recordstoappend:
                                            row = list(row)
                                            if 'Reverified' in fields:
                                                reverified_index = fields.index('Reverified')
                                                row[reverified_index] = 2
                                            insertcursor.insertRow(row)

                    arcpy.management.Delete("NOLocationErrorPE") 

                    arcpy.AddMessage(f"Checking for Point Events That Had No Location Error")

                    arcpy.MakeTableView_management('NDOTPointEvents',"NOLocationErrorPE","(Verified = 1) And (Reverified IS NULL Or Reverified = 1) And (LOWER(MilepostEnd) = 'p' OR MilepostEnd = MilepostBegin) And (LOC_ERROR = 'NO ERROR')")

                    noerror = int(arcpy.management.GetCount("NOLocationErrorPE")[0])
                    arcpy.AddMessage(f"There are: {noerror} records successfully located with no errors")


                    with arcpy.da.SearchCursor("NOLocationErrorPE",['OID@']) as cursor:   # Check if Table is not empty
                        
                        if next(cursor, None) is not None:

                            arcpy.AddMessage(f"Updating reverified field to 2 for all records that were successfuly located with no error")

                            noerrorcontractIDs = {row[0] for row in arcpy.da.SearchCursor('NOLocationErrorPE',['ID'])}  # get a set of all IDS

                            allnoerrorcontractIDs = ", ".join([f"'{str(ids)}'" for ids in noerrorcontractIDs])
                            noerrorcontractIDsquery = f"ID IN ({allnoerrorcontractIDs})"

                            arcpy.MakeTableView_management(input_table, "pointeventsnoerror",noerrorcontractIDsquery)

                            noerrorIds = {row[0] : row[1] for row in arcpy.da.SearchCursor('NOLocationErrorPE',['ID','LOC_ERROR'])} 

                            with arcpy.da.UpdateCursor("pointeventsnoerror",['ID','Reverified','LocError']) as cursor:
                                totalrowsupdated  = 0
                                for row in cursor:
                                    id = row[0]
                                    if id in noerrorIds:
                                        row[1] = 2
                                        row[2] = noerrorIds[id]
                                        cursor.updateRow(row)
                                        totalrowsupdated  += 1
                                        
                            desc = arcpy.Describe(input_table)
                            
                            arcpy.AddMessage(f"A total of {totalrowsupdated} records had the reverified field set to 2 in the input table {desc.name}.")
                            arcpy.management.Delete("pointeventsnoerror")

                        else:
                            arcpy.AddMessage("Since there were no records with no Location Errors, no records had the Reverified field updated.") 

        #################### LINE EVENT ###########################################

                arcpy.AddMessage(f"Starting Make Route Event Layer for Line Events")

                # Set condition for filter verified and reverified records for Line events 

                arcpy.MakeTableView_management(input_table, "verifiedrecordsLE","Verified = 1 And (Reverified IS NULL Or Reverified = 1) And (LOWER(MilepostEnd) <> 'p' AND MilepostEnd <> MilepostBegin)")

                isLineEvent = False 

                with arcpy.da.SearchCursor("verifiedrecordsLE",['OID@']) as cursor:     # Check if table is empty or not

                    if next(cursor, None) is not None:

                        isLineEvent = True
                    

                        if isLineEvent:

                            desc = arcpy.Describe('verifiedrecordsLE')
                            arcpy.AddMessage(f"The input event table is {desc.name}")

                            lineventinput = int(arcpy.management.GetCount("verifiedrecordsLE")[0])
                            arcpy.AddMessage(f"There are: {lineventinput} Line Events records to be processed")

                            eventidfields = ['RouteId','LINE','FROM_MEASURE','TO_MEASURE']    #Modify for Mile post later

                            in_event_properties = "; ".join(eventidfields)

                            arcpy.AddMessage(f"The input line event properties are: {in_event_properties}")
                            arcpy.AddMessage(f"Route FC is  {route_fc}")
                            arcpy.AddMessage(f"Route ID Field is  {route_id_field}")
                        
                            arcpy.lr.MakeRouteEventLayer(route_fc,
                                                        route_id_field,
                                                        'verifiedrecordsLE',
                                                        "; ".join(eventidfields),
                                                        "NDOTLineEvents",
                                                        None,
                                                        "ERROR_FIELD",
                                                        "NO_ANGLE_FIELD",
                                                        "NORMAL",
                                                        "ANGLE",
                                                        "LEFT")    
                            
                            arcpy.AddMessage(f"Make Route Event Layer for Line Event has successfully processed")
                            arcpy.management.Delete("verifiedrecordsLE")

                        
                            # Filter for events located with error 
                        
                            arcpy.MakeTableView_management("NDOTLineEvents","LocationError","Verified = 1 And (Reverified IS NULL Or Reverified = 1) And (LOWER(MilepostEnd) <> 'p' AND MilepostEnd <> MilepostBegin) AND (LOC_ERROR <> 'NO ERROR')")

                            lineventscontractIDs = {row[0] for row in arcpy.da.SearchCursor("LocationError",['ID'])}                 # get a set of all IDS

                            lineeverror = int(arcpy.management.GetCount("LocationError")[0])
                            arcpy.AddMessage(f"How many records with Error: {lineeverror} ")
                           

                            if  lineventscontractIDs:                                                                                             # check if it is a non-empty set
                                arcpy.AddMessage(f"Updating the reverified field in the input table for all records with Location Error to 0")
                                
                                locerrorcontractIDs = ", ".join([f"'{str(ids)}'" for ids in lineventscontractIDs])                               # Loop through all the ids, add quotation marks, and join them to an empty string.
                                contractIDsquery = f"ID IN ({locerrorcontractIDs})"

                                arcpy.MakeTableView_management(input_table, "lineverifiedrecords",contractIDsquery)

                                locerrorIdsLE = {row[0]: row[1] for row in arcpy.da.SearchCursor('LocationError', ['ID', 'LOC_ERROR'])}

                                with arcpy.da.UpdateCursor("lineverifiedrecords",['ID','Reverified','LocError']) as cursor:
                                    totalrowsupdated  = 0
                                    for row in cursor:
                                        id = row[0]
                                        if id in locerrorIdsLE:
                                            row[1] = 0
                                            row[2] = locerrorIdsLE[id]
                                            cursor.updateRow(row)
                                            totalrowsupdated  += 1 
                                desc = arcpy.Describe(input_table)
                                arcpy.AddMessage(f"A total of {totalrowsupdated} records had the reverified field set to in the input table {desc.name}.")

                            else:
                                arcpy.AddMessage(f"Since there were no records with Location Errors, No record had the reverified field updated 0")

                            arcpy.AddMessage(f"Checking for Line Events that had no Location Error")

                            arcpy.MakeTableView_management("NDOTLineEvents", "NOLocationErrorLE","Verified = 1 And (Reverified IS NULL Or Reverified = 1) And (LOWER(MilepostEnd) <> 'p' AND MilepostEnd <> MilepostBegin) AND (LOC_ERROR = 'NO ERROR')")

                        # Append line event data to historical contract table

                            with arcpy.da.SearchCursor("NOLocationErrorLE",['OID@']) as cursor: 
                                # Check if Table is not empty
                                if next(cursor, None) is not None:
                                        
                                    arcpy.AddMessage(f"Starting to Append records to Historical Contract Line Feature Class.")

                                    nolocerrorfields = {field.name for field in arcpy.ListFields("NOLocationErrorLE")if field.type not in ("OID")}  
                                    lineeventfields = {field.name for field in arcpy.ListFields(Line_fc)if field.type not in ("OID")}
                                    fields = ['SHAPE@'] + list(nolocerrorfields & lineeventfields) # Get fields that are common in both tables, and add geometry field

                                    # arcpy.AddMessage(f"{fields}")

                                    indexofEventId = fields.index('ID')  # Get the index of the contract number in the fields table (use this index in the search cursor)
                                    lineeventrecordstoappend = {row[0] for row in arcpy.da.SearchCursor(Line_fc,['ID'])}  #Get all Contract Numbers in the tables located with no error 
                                
                                    with arcpy.da.InsertCursor(Line_fc,fields) as insertcursor:
                                        with arcpy.da.SearchCursor("NOLocationErrorLE",fields) as searchcursor:
                                            for row in searchcursor:
                                                if str(row[indexofEventId]) not in lineeventrecordstoappend:
                                                    row = list(row)
                                                    if 'Reverified' in fields:
                                                      reverified_index = fields.index('Reverified')
                                                      row[reverified_index] = 2
                                                    insertcursor.insertRow(row)

                            arcpy.MakeTableView_management("NDOTLineEvents", "NOLocationErrorLE","Verified = 1 And (Reverified IS NULL Or Reverified = 1) And (LOWER(MilepostEnd) <> 'p' AND MilepostEnd <> MilepostBegin) AND (LOC_ERROR = 'NO ERROR')")
                            lineeventsnoerror = int(arcpy.management.GetCount("NOLocationErrorLE")[0])
                            arcpy.AddMessage(f"There are: {lineeventsnoerror} records successfully located with no errors")

                            with arcpy.da.SearchCursor("NOLocationErrorLE",['OID@']) as cursor:   # Check if Table is not empty
                                if next(cursor, None) is not None:
                                     
                                     arcpy.AddMessage(f"Updating reverified field to 2 for all records that were successfuly located with no error")

                                     noerrorcontractIDs = {row[0] for row in arcpy.da.SearchCursor('NOLocationErrorLE',['ID'])}  # get a set of all IDS
                                     
                                     allnoerrorcontractIDs = ", ".join([f"'{str(ids)}'" for ids in noerrorcontractIDs])
                                     noerrorcontractIDsquery = f"ID IN ({allnoerrorcontractIDs})"

                                     arcpy.MakeTableView_management(input_table, "lineeventsnoerror",noerrorcontractIDsquery)

                                     nolocerrorIdsLE = {row[0]: row[1] for row in arcpy.da.SearchCursor('NOLocationErrorLE', ['ID', 'LOC_ERROR'])}

                                     with arcpy.da.UpdateCursor("lineeventsnoerror",['ID','Reverified','LocError']) as cursor:  
                                          totalrowsupdated  = 0
                                          for row in cursor:
                                            id = row[0]
                                            if id in nolocerrorIdsLE:
                                               row[1] = 2
                                               row[2] = nolocerrorIdsLE[id]
                                               cursor.updateRow(row)
                                               totalrowsupdated  += 1
                                     desc = arcpy.Describe(input_table)

                                     arcpy.AddMessage(f"A total of {totalrowsupdated} records had the reverified field set to 2 in the input table {desc.name}.")
                                     arcpy.management.Delete("lineeventsnoerror")
                                else:
                                    arcpy.AddMessage("Since there were no records with no Location Errors, no records had the Reverified field updated.")

                            arcpy.management.Delete("NOLocationErrorLE") 
                    else:
                        arcpy.AddMessage('No Line Event records to Process')
                
                    # Delete fields that were added at the begining 

                    arcpy.AddMessage(f"Deleting temporary fields")

                    arcpy.MakeTableView_management(input_table, "verifiedrecords")
                    
                    arcpy.management.DeleteField("verifiedrecords","FROM_MEASURE;TO_MEASURE","DELETE_FIELDS") 
            else:
                arcpy.AddMessage("There are no verified records to process at this time.")



            end_time = time.time()
            execution_time = end_time - start_time
            arcpy.AddMessage(f"Execution time: {execution_time:.2f} seconds")


        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return
