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

        # Set condition for verified and reverified records then filter table for INVERSE of the condition 

        arcpy.management.SelectLayerByAttribute("verifiedrecords","NEW_SELECTION","Verified = 1 Or Reverified IS NULL Or Reverified = 1",'INVERT')

        hasunverified = any(arcpy.da.SearchCursor("verifiedrecords", ["OID@"]))
        
        # Set Reverify to 0 when condition is true
        if hasunverified:
            Totalunverified = arcpy.management.GetCount('verifiedrecords')
            arcpy.AddMessage(f"Number of Unverified Records: {Totalunverified[0]}")

            with arcpy.da.UpdateCursor('verifiedrecords',['Reverified']) as cursor:
                for row in cursor:
                    row[0] = 0             # Set Reverified to 0 in input table 
                    cursor.updateRow(row)
        else:
             arcpy.AddMessage("No unverified records found. No updates made.")

        # clear selection on records     
            
        arcpy.management.SelectLayerByAttribute("verifiedrecords", "CLEAR_SELECTION")


       # Set condition for verified and reverified records then filter table 

        arcpy.management.SelectLayerByAttribute("verifiedrecords","NEW_SELECTION","(Verified = 1 OR Reverified IS NULL OR Reverified = 1) AND (LOWER(MilepostEnd) = 'p' OR MilepostEnd = MilepostBegin)",None)
        
         
        # Check if table is a point event table 

        isPointEvent = False 
        with arcpy.da.SearchCursor('verifiedrecords',['OID@'])  as cursor: 
           if next(cursor, None) is not None :
                    isPointEvent = True

#################### POINT EVENT ####################################
        # Make route event layer
        if isPointEvent:
            arcpy.AddMessage(f"This is a Point Event Table")
            arcpy.AddMessage(f"Starting Make Route Event Layer For The Point Events")

            eventidfields = ['RouteId','POINT','Measure']

            in_event_properties = "; ".join(eventidfields)

            arcpy.AddMessage(f"The input event properties are: {in_event_properties}")
            arcpy.AddMessage(f"Route FC is  {route_fc}")
            arcpy.AddMessage(f"Route ID Field is  {route_id_field}")
            arcpy.AddMessage(f"Event table is  {'verifiedrecords'}")
            desc = arcpy.Describe('verifiedrecords')
            arcpy.AddMessage(f"The input event is {desc.name} and it's a {desc.datasetType}")

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
            
            arcpy.management.SelectLayerByAttribute("verifiedrecords", "CLEAR_SELECTION")

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

            arcpy.MakeTableView_management(f"NDOTPointEvents{datetime.now().strftime('%d')}","LocationError")

            arcpy.management.SelectLayerByAttribute('LocationError', "NEW_SELECTION","LOC_ERROR <> 'NO ERROR'")

            locerrorpoints = int(arcpy.management.GetCount('LocationError')[0])
            arcpy.AddMessage(f"Number of records that had Location Error: {locerrorpoints}")

            # Get contract IDs for all events where there is Location

            contractIDs = {row[0] for row in arcpy.da.SearchCursor('LocationError',['ContractNumber'])}                 # get a set of all IDS

            if contractIDs:                                                                                             # check if it is a non-empty set
                arcpy.AddMessage(f"Updating the reverified field in the input table for all records with Location Error to 0")  

                locerrorcontractIDs = ", ".join([f"'{str(ids)}'" for ids in contractIDs])                               # Loop through all the ids, add quotation marks, and join them to an empty string.
                contractIDsquery = f"ContractNumber IN ({locerrorcontractIDs})"
                arcpy.management.SelectLayerByAttribute("verifiedrecords", "NEW_SELECTION",contractIDsquery )

                with arcpy.da.UpdateCursor("verifiedrecords",['Reverified']) as cursor:
                    totalrowsupdated  = 0
                    for row in cursor:
                        row[0] = 3
                        cursor.updateRow(row)
                        totalrowsupdated  += 1

                arcpy.AddMessage(f"A total of {totalrowsupdated} records in had the reverified field set to in the input table: '{'verifiedrecords'}'.")

                arcpy.management.SelectLayerByAttribute("verifiedrecords", "CLEAR_SELECTION")

            else:
                arcpy.AddMessage(f"Since there were no records Locatioon Location Errors, No record had the revirified field updated 0")


            # Filter for events that have No Error 
            
            # error_condition = "LOC_ERROR = 'NO ERROR'"

            nolocerror = arcpy.management.SelectLayerByAttribute("LocationError", "NEW_SELECTION", "LOC_ERROR = 'NO ERROR'")

            noerror = int(arcpy.management.GetCount(nolocerror)[0])
            arcpy.AddMessage(f"There are: {noerror} records successfully located with no errors")

            # Append point event data to historical contract table

            with arcpy.da.SearchCursor(nolocerror,['OID@']) as cursor:
                if next(cursor, None) is not None:
                
                    fieldmapping = arcpy.FieldMappings()
                    fieldmapping.addTable(nolocerror)
                    fieldmapping.addTable(Point_fc)


                    arcpy.management.Append(inputs=nolocerror,
                                        target= Point_fc,
                                        schema_type="NO_TEST",    # modify to TEST later
                                        field_mapping=fieldmapping,
                                        subtype="",
                                        expression="",
                                        match_fields=None,
                                        update_geometry="NOT_UPDATE_GEOMETRY",
                                        enforce_domains="NO_ENFORCE_DOMAINS")
        
#################### LINE EVENT ###########################################

        arcpy.AddMessage(f"Starting Make Route Event Layer for Line Event")
        # Set condition for filter verified and reverified records for Line events 

        arcpy.management.SelectLayerByAttribute("verifiedrecords", "CLEAR_SELECTION")

        arcpy.management.SelectLayerByAttribute("verifiedrecords","NEW_SELECTION","(Verified = 1 OR Reverified IS NULL OR Reverified = 1) AND (LOWER(MilepostEnd) <> 'p' OR MilepostEnd <> MilepostBegin)",None)

        lineventinput = int(arcpy.management.GetCount("verifiedrecords")[0])
        arcpy.AddMessage(f"There are: {lineventinput} records to be processed")
        
        with arcpy.da.SearchCursor("verifiedrecords",['OID@']) as cursor:

            if next(cursor, None) is not None:

                lineventinput = int(arcpy.management.GetCount("verifiedrecords")[0])
                arcpy.AddMessage(f"There are: {lineventinput} records to be processed")

                eventidfields = ['RouteId','LINE','FROM_MEASURE','TO_MEASURE']    #Modify for Mile post later

                in_event_properties = "; ".join(eventidfields)

                arcpy.AddMessage(f"The input event properties are: {in_event_properties}")
                arcpy.AddMessage(f"Route FC is  {route_fc}")
                arcpy.AddMessage(f"Route ID Field is  {route_id_field}")
                arcpy.AddMessage(f"Event table is  {'verifiedrecords'}")
                desc = arcpy.Describe('verifiedrecords')
                arcpy.AddMessage(f"The input event is {desc.name} and it's a {desc.datasetType}")

                arcpy.lr.MakeRouteEventLayer(route_fc,
                                            route_id_field,
                                            'verifiedrecords',
                                            "; ".join(eventidfields),
                                            f"NDOTLineEvents{datetime.now().strftime('%d_%H')}",
                                            None,
                                            "ERROR_FIELD",
                                            "NO_ANGLE_FIELD",
                                            "NORMAL",
                                            "ANGLE",
                                            "LEFT",
                                            "POINT")    # verify with team if Line or Point for this field
                
                arcpy.AddMessage(f"Make Route Event Layer for Line Event has successfully processed")

                
        # Display Point Event Layer in Current Map

                desc = arcpy.Describe(f"NDOTLineEvents{datetime.now().strftime('%d_%H')}")
                arcpy.AddMessage(f"The input event is {desc.name} and it's a {desc.datasetType}")

                aprx = arcpy.mp.ArcGISProject("CURRENT")
                m = aprx.activeMap
                
                lineeventname = f"NDOTLineEvents{datetime.now().strftime('%d_%H')}"

                layer = arcpy.management.MakeFeatureLayer(lineeventname,'Historical Contract Line Events')[0]  

                if addline == 'true':

                    if arcpy.Exists(layer):
                        m.addLayer(layer)
                        arcpy.AddMessage(f"Historical Contract Line Events Layer has successfully being added")

                    else:
                        arcpy.AddMessage(f"{lineeventname} does not exist.")


            
                # Filter for events located with error 
            
                arcpy.MakeTableView_management(f"NDOTLineEvents{datetime.now().strftime('%d_%H')}","LocationError")

                locerror = arcpy.management.SelectLayerByAttribute("LocationError", "NEW_SELECTION", "LOC_ERROR <> 'NO ERROR'")

                lineventscontractIDs = {row[0] for row in arcpy.da.SearchCursor('LocationError',['ContractNumber'])}                 # get a set of all IDS

                if  lineventscontractIDs:                                                                                             # check if it is a non-empty set
                    locerrorcontractIDs = ", ".join([f"'{str(ids)}'" for ids in lineventscontractIDs])                               # Loop through all the ids, add quotation marks, and join them to an empty string.
                    contractIDsquery = f"ContractNumber IN ({locerrorcontractIDs})"
                    arcpy.management.SelectLayerByAttribute("verifiedrecords", "NEW_SELECTION",contractIDsquery )

                    with arcpy.da.UpdateCursor("verifiedrecords",['Reverified']) as cursor:
                        totalrowsupdated  = 0
                        for row in cursor:
                            row[0] = 7
                            cursor.updateRow(row)
                            totalrowsupdated  += 1

                    arcpy.AddMessage(f"Updated {totalrowsupdated} records in '{'verifiedrecords'}'.")

                    arcpy.management.SelectLayerByAttribute("verifiedrecords", "CLEAR_SELECTION")

                else:
                    arcpy.AddMessage(f"No Location Errors, No records updated")


                arcpy.management.SelectLayerByAttribute("LocationError", "CLEAR_SELECTION")

                nolocerror = arcpy.management.SelectLayerByAttribute("LocationError", "NEW_SELECTION", "LOC_ERROR = 'NO ERROR'")

            # Append point event data to historical contract table

                arcpy.AddMessage(f"Starting to Append records to Historical Contract Feature .")

                with arcpy.da.SearchCursor(nolocerror,['OID@']) as cursor:
                    if next(cursor, None) is not None:
                        fieldmapping = arcpy.FieldMappings()
                        fieldmapping.addTable(nolocerror)
                        fieldmapping.addTable(Line_fc)

                        arcpy.management.Append(inputs=nolocerror,
                                            target= Line_fc,
                                            schema_type="NO_TEST",    # modify to TEST later
                                            field_mapping=fieldmapping,
                                            subtype="",
                                            expression="",
                                            match_fields=None,
                                            update_geometry="NOT_UPDATE_GEOMETRY",
                                            enforce_domains="NO_ENFORCE_DOMAINS")
                        

                    arcpy.conversion.ExportTable(
                                        in_table=nolocerror,
                                        out_table='WHAfT',
                                        where_clause="",
                                        use_field_alias_as_name="NOT_USE_ALIAS",
                                        field_mapping='',
                                        sort_field=None
                                    )
            
        end_time = time.time()
        execution_time = end_time - start_time
        arcpy.AddMessage(f"Execution time: {execution_time:.2f} seconds")


        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return
