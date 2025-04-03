# -*- coding: utf-8 -*-

import arcpy
import pandas as pd
import numpy as np
import os
from datetime import datetime
import time


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

        inputSDE = arcpy.Parameter( displayName="Input Table",  #Input SDE Table
                             name="input_table",
                             datatype="DETable",
                             parameterType="Required",
                             direction="Input")
        inputSDE.description = "Select the SDE table that contains the data for the processing tool."
        
        inputRouteFC = arcpy.Parameter(displayName="Input Route Feature Class",  #The route features on which events will be located.
                            name="route_fc",
                            datatype="DEFeatureClass",     
                            parameterType="Required",
                            direction="Input")
        inputRouteFC.description = "Select the route feature class containing the routes to be used in the event generation."

        
        routeIDField =  arcpy.Parameter(displayName="Route Identifier Field",    #The field containing values that uniquely identify each route
                              name="route_id_field",
                              datatype="Field",
                              parameterType="Required",
                              direction="Input")
        routeIDField.parameterDependencies = [inputRouteFC.name]                 #Set dependencies for Route ID field using the route feature class
        routeIDField.value = 'RouteId'                                           # Set default value for Route ID field to RouteId
        routeIDField.description = "Select the field in the Route Feature Class that uniquely identifies each route (e.g., RouteId)."
        
        pointEvent =  arcpy.Parameter(displayName="Scoping Contract Points", 
                                   name = 'Point_fc',
                                   datatype="DEFeatureClass",
                                   parameterType="Optional",
                                   direction="Input")
        pointEvent.description =  "Select the Historical Contract Point Feature class to which the generated point events will be appended."

        lineEvent =  arcpy.Parameter(displayName="Scoping Contract Line", 
                                name = 'Line_fc',
                                datatype="DEFeatureClass",
                                parameterType="Optional",
                                direction="Input")
    
        lineEvent.description =  "Select the Historical Contract Line Feature class to which the generated line events will be appended."
    
        
        addpointevent = arcpy.Parameter(displayName="Add Point Event Layer To Map", 
                                name = 'addpointevent_layer',
                                datatype="Boolean",
                                parameterType="Optional",
                                direction="Input")
        addpointevent.description = "Check this option to add the generated point events layer to the active map."
        
        addlineevent = arcpy.Parameter(displayName="Add Line Event Layer To Map", 
                                name = 'addlineevent_layer',
                                datatype="Boolean",
                                parameterType="Optional",
                                direction="Input")
        addlineevent.description = "Check this option to add the generated line events layer to the active map."
                                                    
        
        params = [inputSDE,inputRouteFC,routeIDField,pointEvent,lineEvent,addpointevent,addlineevent]

        return params

    def isLicensed(self):
        """Set whether the tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
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

        # Set parameters for the input 
        input_table = parameters[0].valueAsText    # input SDE Table 
        route_fc = parameters[1].valueAsText        #The route features on which events will be located.
        route_id_field = parameters[2].valueAsText  # The field containing values that uniquely identify each route 
        Point_fc = parameters[3].valueAsText  # Point Event Feature class to which events will be appended
        Line_fc = parameters[4].valueAsText  # Line Event Feature class to which events will be appended
        addpoint = parameters[5].valueAsText  # Check box for adding point event layer to map
        addline = parameters[6].valueAsText  # Check box for adding line event layer to map

       
        # Make a temporary table from input table

        arcpy.MakeTableView_management(input_table, "verifiedrecords")

        inputrecordscount = int(arcpy.management.GetCount("verifiedrecords")[0])
        arcpy.AddMessage(f"There are: {inputrecordscount} records in the input table")

        # Add Field 
        
        #Add Measure 

        if 'FROM_MEASURE' not in [f.name for f in arcpy.ListFields("verifiedrecords")]: 
            arcpy.AddField_management("verifiedrecords", 'FROM_MEASURE', "DOUBLE",10,5,None,'FROM_MEASURE')
            arcpy.CalculateField_management("verifiedrecords", 'FROM_MEASURE',  "None if str(!MilepostBegin!).isalpha() else float(!MilepostBegin!)", "PYTHON3")

        if 'TO_MEASURE' not in [f.name for f in arcpy.ListFields("verifiedrecords")]: 
            
            arcpy.AddField_management("verifiedrecords", 'TO_MEASURE', "DOUBLE",10,5,None,'TO_MEASURE')
            arcpy.CalculateField_management("verifiedrecords", 'TO_MEASURE',  "None if str(!MilepostEnd!).isalpha() else float(!MilepostEnd!)", "PYTHON3")
        
        

        # Set condition for verified and reverified records then filter table for INVERSE of the condition 

        arcpy.management.SelectLayerByAttribute("verifiedrecords","NEW_SELECTION","Verified = 1 Or Reverified IS NULL Or Reverified = 1",'INVERT')

        hasunverified = any(arcpy.da.SearchCursor("verifiedrecords", ["OID@"]))
        
        # Set Reverify to 0 when condition is true
        if hasunverified:
            Totalunverified = arcpy.management.GetCount('verifiedrecords')
            arcpy.AddMessage(f"There are: {Totalunverified[0]} unverified records")

            with arcpy.da.UpdateCursor('verifiedrecords',['Reverified']) as cursor:
                for row in cursor:
                    row[0] = 0            # Set Reverified to 0 in input table 
                    cursor.updateRow(row)
        else:
             arcpy.AddMessage("No unverified records found. No updates made.")

        # clear selection on records     
            
        arcpy.management.SelectLayerByAttribute("verifiedrecords", "CLEAR_SELECTION")


       # Set condition for verified and reverified records then filter table 

        arcpy.MakeTableView_management(input_table, "verifiedrecords","(Verified = 1 OR Reverified IS NULL OR Reverified = 1) AND (LOWER(MilepostEnd) = 'p' OR MilepostEnd = MilepostBegin)")
        

        totalpointeventrecords = arcpy.management.GetCount('verifiedrecords')
        arcpy.AddMessage(f"There are: {totalpointeventrecords[0]} point event records to be processed")

        # arcpy.management.SelectLayerByAttribute("verifiedrecords","NEW_SELECTION","(Verified = 1 OR Reverified IS NULL OR Reverified = 1) AND (LOWER(MilepostEnd) = 'p' OR MilepostEnd = MilepostBegin)",None)
        
         
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
            arcpy.AddMessage(f"This table has Point Events")
            arcpy.AddMessage(f"Starting Make Route Event Layer For The Point Events")

            totalpointeventrecords = arcpy.management.GetCount('verifiedrecords')
            arcpy.AddMessage(f"There are: {totalpointeventrecords[0]} point event records to be processed")

            eventidfields = ['RouteId','POINT','TO_MEASURE']

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
                                         f"NDOTPointEvents{datetime.now().strftime('%d')}",
                                         None,
                                         "ERROR_FIELD",
                                         "NO_ANGLE_FIELD",
                                         "NORMAL",
                                         "ANGLE",
                                         "LEFT",
                                         "POINT")
            arcpy.AddMessage(f"Make Route Event Layer for Point Events has successfully processed")
            
            arcpy.management.Delete("verifiedrecords")

            # Display Point Event Layer in Current Map

            aprx = arcpy.mp.ArcGISProject("CURRENT")
            m = aprx.activeMap
            
            pointeventname = f"NDOTPointEvents{datetime.now().strftime('%d')}"

            desc = arcpy.Describe(f"NDOTPointEvents{datetime.now().strftime('%d')}")
            arcpy.AddMessage(f"The input event is {desc.name} and it's a {desc.datasetType}")

            layer = arcpy.management.MakeFeatureLayer(pointeventname,'Historical Contract Point Events')[0] 

            if addpoint == 'true':

                if arcpy.Exists(layer):
                    m.addLayer(layer)
                    arcpy.AddMessage(f"Point Event Layer has successfully being added to the map")

                else:
                    arcpy.AddMessage(f"{pointeventname} does not exist.")
            

            # Filter for events located with error 
            arcpy.AddMessage(f"Check for Point Events that had Location Error")

            # arcpy.MakeTableView_management(f"NDOTPointEvents{datetime.now().strftime('%d')}","LocationError","LOC_ERROR <> 'NO ERROR'")

            arcpy.MakeTableView_management(f"NDOTPointEvents{datetime.now().strftime('%d')}","LocationError","( (Verified = 1 OR Reverified IS NULL OR Reverified = 1) AND (LOWER(MilepostEnd) = 'p' OR MilepostEnd = MilepostBegin) AND LOC_ERROR <> 'NO ERROR')")

            # arcpy.management.SelectLayerByAttribute("LocationError", "NEW_SELECTION", "( (Verified = 1 OR Reverified IS NULL OR Reverified = 1) AND (LOWER(MilepostEnd) = 'p' OR MilepostEnd = MilepostBegin) AND LOC_ERROR <> 'NO ERROR')")


            locerrorpoints = int(arcpy.management.GetCount('LocationError')[0])
            arcpy.AddMessage(f"Number of records that had Location Error: {locerrorpoints}")

            # Get contract IDs for all events where there is Location

            contractIDs = {row[0] for row in arcpy.da.SearchCursor('LocationError',['RouteId'])}                 # get a set of all IDS

            arcpy.AddMessage(f"How many ids: {len(contractIDs)} ")

            if contractIDs:                                                                                             # check if it is a non-empty set
                arcpy.AddMessage(f"Updating the reverified field in the input table for all records with Location Error to 0")  

                locerrorcontractIDs = ", ".join([f"'{str(ids)}'" for ids in contractIDs])                               # Loop through all the ids, add quotation marks, and join them to an empty string.
                contractIDsquery = f"RouteId IN ({locerrorcontractIDs})"
                arcpy.MakeTableView_management(input_table, "pointverifiedrecords",contractIDsquery)

                arcpy.AddMessage(f"Updating records with these IDs: {locerrorcontractIDs} ")
                # arcpy.management.SelectLayerByAttribute("verifiedrecords", "NEW_SELECTION",contractIDsquery )


                with arcpy.da.UpdateCursor("pointverifiedrecords",['Reverified']) as cursor:
                    totalrowsupdated  = 0
                    for row in cursor:
                        row[0] = 444
                        cursor.updateRow(row)
                        totalrowsupdated  += 1
                desc = arcpy.Describe(input_table)
                arcpy.AddMessage(f"A total of {totalrowsupdated} records had the reverified field set to in the input table {desc.name}.")

                # arcpy.management.SelectLayerByAttribute("verifiedrecords", "CLEAR_SELECTION")
                arcpy.management.Delete("pointverifiedrecords")

            else:
                arcpy.AddMessage(f"Since there were no records with Location Errors, No record had the reverified field updated 0")


            # Filter for events that have No Error 
            
            # error_condition = "LOC_ERROR = 'NO ERROR'"

            arcpy.AddMessage(f"Checking for Point Events that had no Location Error")

            nolocerror = arcpy.management.SelectLayerByAttribute("LocationError", "NEW_SELECTION", "LOC_ERROR = 'NO ERROR'")

            noerror = int(arcpy.management.GetCount(nolocerror)[0])
            arcpy.AddMessage(f"There are: {noerror} records successfully located with no errors")

            arcpy.AddMessage(f"Number of input records: {inputrecordscount},\nNumber of Records located with No Errors: {noerror}\nDifference: {inputrecordscount -noerror }")
          
            # Append point event data to historical contract table

            with arcpy.da.SearchCursor(nolocerror,['OID@']) as cursor:   # Check if Table is not empty
                if next(cursor, None) is not None:
                    arcpy.AddMessage(f"Appending records that were located with no errors to Point Event feature class")

                    nolocerrorfields = {field.name for field in arcpy.ListFields(nolocerror)if field.type not in ("OID")}  
                    pointeventfields = {field.name for field in arcpy.ListFields(Point_fc)if field.type not in ("OID")}
                    fields = ['SHAPE@'] + list(nolocerrorfields & pointeventfields) # Get fields that are common in both tables, and add geometry field

                    arcpy.AddMessage(f"{fields}")

                    indexofEventId = fields.index('EventId')  # Get the index of the contract number in the fields table (use this index in the search cursor)
                    recordstoappend = {row[0] for row in arcpy.da.SearchCursor(Point_fc,['EventId'])}  #Get all Contract Numbers in the tables located with no error 
                
                   
                    with arcpy.da.InsertCursor(Point_fc,fields) as insertcursor:
                        with arcpy.da.SearchCursor(nolocerror,fields) as searchcursor:
                            for row in searchcursor:
                                if row[indexofEventId] not in recordstoappend:
                                    insertcursor.insertRow(row)
        
#################### LINE EVENT ###########################################

        arcpy.AddMessage(f"Starting Make Route Event Layer for Line Events")

        # Set condition for filter verified and reverified records for Line events 

        # arcpy.management.SelectLayerByAttribute("verifiedrecords", "CLEAR_SELECTION")

        arcpy.MakeTableView_management(input_table, "verifiedrecords","(Verified = 1 OR Reverified IS NULL OR Reverified = 1) AND (LOWER(MilepostEnd) <> 'p' OR MilepostEnd <> MilepostBegin)")

        # arcpy.management.SelectLayerByAttribute("verifiedrecords","NEW_SELECTION","(Verified = 1 OR Reverified IS NULL OR Reverified = 1) AND (LOWER(MilepostEnd) <> 'p' OR MilepostEnd <> MilepostBegin)",None)

        isLineEvent = False 

        with arcpy.da.SearchCursor("verifiedrecords",['OID@']) as cursor:     # Check if table is empty or not

            if next(cursor, None) is not None:

                isLineEvent = True
            

                if isLineEvent:

                    desc = arcpy.Describe('verifiedrecords')
                    arcpy.AddMessage(f"The input event table is {desc.name}")

                    lineventinput = int(arcpy.management.GetCount("verifiedrecords")[0])
                    arcpy.AddMessage(f"There are: {lineventinput} Line Events records to be processed")

                    eventidfields = ['RouteId','LINE','FROM_MEASURE','TO_MEASURE']    #Modify for Mile post later

                    in_event_properties = "; ".join(eventidfields)

                    arcpy.AddMessage(f"The input line event properties are: {in_event_properties}")
                    arcpy.AddMessage(f"Route FC is  {route_fc}")
                    arcpy.AddMessage(f"Route ID Field is  {route_id_field}")
                   
                    arcpy.lr.MakeRouteEventLayer(route_fc,
                                                route_id_field,
                                                'verifiedrecords',
                                                "; ".join(eventidfields),
                                                f"NDOTLineEvents{datetime.now().strftime('%d')}",
                                                None,
                                                "ERROR_FIELD",
                                                "NO_ANGLE_FIELD",
                                                "NORMAL",
                                                "ANGLE",
                                                "LEFT",
                                                "POINT")    # verify with team if Line or Point for this field
                    
                    arcpy.AddMessage(f"Make Route Event Layer for Line Event has successfully processed")
                    arcpy.management.Delete("verifiedrecords")


                    
            # Display Point Event Layer in Current Map

                    aprx = arcpy.mp.ArcGISProject("CURRENT")
                    m = aprx.activeMap
                    
                    lineeventname = f"NDOTLineEvents{datetime.now().strftime('%d')}"

                    layer = arcpy.management.MakeFeatureLayer(lineeventname,'Historical Contract Line Events')[0]  

                    if addline == 'true':

                        if arcpy.Exists(layer):
                            m.addLayer(layer)
                            arcpy.AddMessage(f"Historical Contract Line Events Layer has successfully being added")
                        else:
                            None

                
                    # Filter for events located with error 
                
                    arcpy.MakeTableView_management(f"NDOTLineEvents{datetime.now().strftime('%d')}","LocationError",)

                    arcpy.management.SelectLayerByAttribute("LocationError", "NEW_SELECTION", "( (Verified = 1 OR Reverified IS NULL OR Reverified = 1) AND (LOWER(MilepostEnd) <> 'p' OR MilepostEnd <> MilepostBegin) AND LOC_ERROR <> 'NO ERROR')")

                    lineventscontractIDs = {row[0] for row in arcpy.da.SearchCursor("LocationError",['RouteId'])}                 # get a set of all IDS

                    lineeverror = int(arcpy.management.GetCount("LocationError")[0])
                    arcpy.AddMessage(f"How many records with Error: {lineeverror} ")
                    arcpy.AddMessage(f"How many Unique ids: {len(lineventscontractIDs)} ")

                    if  lineventscontractIDs:                                                                                             # check if it is a non-empty set
                        arcpy.AddMessage(f"Updating the reverified field in the input table for all records with Location Error to 0")
                        
                        locerrorcontractIDs = ", ".join([f"'{str(ids)}'" for ids in lineventscontractIDs])                               # Loop through all the ids, add quotation marks, and join them to an empty string.
                        contractIDsquery = f"RouteId IN ({locerrorcontractIDs})"

                        arcpy.AddMessage(f"Updating records with these IDs: {lineventscontractIDs} ")

                        arcpy.MakeTableView_management(input_table, "lineverifiedrecords",contractIDsquery)
                        # arcpy.management.SelectLayerByAttribute("verifiedrecords", "NEW_SELECTION",contractIDsquery )

                        with arcpy.da.UpdateCursor("lineverifiedrecords",['Reverified']) as cursor:
                            totalrowsupdated  = 0
                            for row in cursor:
                                row[0] = 789
                                cursor.updateRow(row)
                                totalrowsupdated  += 1
                                
                        desc = arcpy.Describe(input_table)
                        arcpy.AddMessage(f"A total of {totalrowsupdated} records had the reverified field set to in the input table {desc.name}.")
                        # arcpy.management.SelectLayerByAttribute("verifiedrecords", "CLEAR_SELECTION")                       # Clear selection from table     
                        arcpy.management.Delete("lineverifiedrecords")  
                               

                    else:
                        arcpy.AddMessage(f"Since there were no records with Location Errors, No record had the reverified field updated 0")

                    arcpy.management.SelectLayerByAttribute("LocationError", "CLEAR_SELECTION")                                      # Clear selection from table 

                    arcpy.AddMessage(f"Checking for Line Events that had no Location Error")

                    nolocerror = arcpy.management.SelectLayerByAttribute("LocationError", "NEW_SELECTION", "LOC_ERROR = 'NO ERROR'")

                    lineeventsnoerror = int(arcpy.management.GetCount(nolocerror)[0])
                    arcpy.AddMessage(f"There are: {lineeventsnoerror} records successfully located with no errors")

                    arcpy.AddMessage(f"Number of input records: {inputrecordscount},\nNumber of line events located with No Errors: {lineeventsnoerror}\nDifference: {inputrecordscount -lineeventsnoerror}")
          

                # Append point event data to historical contract table

                    with arcpy.da.SearchCursor(nolocerror,['OID@']) as cursor: 
                          # Check if Table is not empty
                        if next(cursor, None) is not None:
                                
                            arcpy.AddMessage(f"Starting to Append records to Historical Contract Line Feature Class.")

                            nolocerrorfields = {field.name for field in arcpy.ListFields(nolocerror)if field.type not in ("OID")}  
                            lineeventfields = {field.name for field in arcpy.ListFields(Line_fc)if field.type not in ("OID")}
                            fields = ['SHAPE@'] + list(nolocerrorfields & lineeventfields) # Get fields that are common in both tables, and add geometry field

                            arcpy.AddMessage(f"{fields}")

                            indexofEventId = fields.index('RouteId')  # Get the index of the contract number in the fields table (use this index in the search cursor)
                            lineeventrecordstoappend = {row[0] for row in arcpy.da.SearchCursor(Line_fc,['RouteId'])}  #Get all Contract Numbers in the tables located with no error 
                        
                        
                            with arcpy.da.InsertCursor(Line_fc,fields) as insertcursor:
                                with arcpy.da.SearchCursor(nolocerror,fields) as searchcursor:
                                    for row in searchcursor:
                                        if row[indexofEventId] not in lineeventrecordstoappend:
                                            insertcursor.insertRow(row)
            else:
                arcpy.AddMessage('No Line Event records to Process')
        
        # Delete fields that were added at the begining 

        # arcpy.AddMessage(f"Deleting temporary fields")

        # arcpy.MakeTableView_management(input_table, "verifiedrecords")
        
        # arcpy.management.DeleteField("verifiedrecords","FROM_MEASURE;TO_MEASURE","DELETE_FIELDS") 


        end_time = time.time()
        execution_time = end_time - start_time
        arcpy.AddMessage(f"Execution time: {execution_time:.2f} seconds")


        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return
