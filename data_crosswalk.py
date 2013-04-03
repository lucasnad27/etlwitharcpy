from arcpy import (
    env,
    management,
    da,
    ListFeatureClasses,
    ListFields
)
import os
import logging
import json

env.workspace = r'path\to\sde\file'
local_workspace = r'local\path\to\directory\of\gdbs'
config_location = r'config\file\location'

logging.basicConfig(
    filename=os.path.join(config_location, 'data_crosswalk.log'),
    level=logging.INFO,
    filemode='w')


def main():
    # table_wipe()
    assign_domains()
    data_prep()
    clear_data()
    migrate_data()
    #DEVNOTE:
    #when populating data, check for [' ', "", " ", "NULL", "null"] and change to NULL
    #when populating data, trim whitespace


def clear_data():
    # Clear data out of all production feature classes
    logging.info('Clearing all feature class data from %s' % env.workspace)
    fcs = ListFeatureClasses()
    for fc in fcs:
        if 'PARCELS' in fc.upper() or 'SUBDIVISION' in fc.upper():
            logging.info('Retaining %s' % fc)
        else:
            management.DeleteFeatures(fc)


def migrate_data():
    config = json.load(open(os.path.join(config_location, 'crosswalk.json')))
    local_workspace = config['localpath']
    for gdb in config['geodatabases']:
        logging.info('Transferring data from %s' % gdb['name'])
        for table in gdb['tables']:
            #Repair any potential geometry issues before inserting into SDE
            local_table = os.path.join(local_workspace,
                os.path.join(gdb['name'], table['name']))
            management.RepairGeometry(local_table)
            new_fields = []
            old_fields = []
            if 'defaults' in table.keys():
                add_local_default(
                    local_table,
                    table)
                for default in table['defaults']:
                    new_fields.append(default['field'])
                    old_fields.append(default['field'])
            logging.info('  Transferring data from %s into %s' % (
                table['name'], table['new_table']))
            sde_table = os.path.join(env.workspace, table['new_table'])
            for field in table['fields']:
                new_fields.append(field['new_field'])
                old_fields.append(field['name'])
            new_fields.append("SHAPE@")
            old_fields.append("SHAPE@")
            insert_cursor = da.InsertCursor(sde_table, new_fields)
            count = 0
            with da.SearchCursor(local_table, old_fields) as search_cursor:
                for row in search_cursor:
                    try:
                        logging.debug(row)
                        insert_cursor.insertRow(row)
                        count += 1
                    except (RuntimeError), e:
                        logging.warning(e)
                        logging.warning(row)

            logging.info('Successfully added %s records to %s from %s' %
                (count, os.path.basename(sde_table), os.path.basename(local_table)))
            # Traversal example
            # print '    Old table: %s; New table: %s' % (table['name'], table['new_table'])
            # for field in table['fields']:
            #     print '        Old field: %s; New Field: %s' % (field['name'], field['new_field'])


def add_local_default(path, table):
    for default in table['defaults']:
        logging.info('Adding default values for %s' % os.path.basename(path))
        management.AddField(path, default['field'])
        management.CalculateField(path, default['field'], default['default'])


def assign_domains():
    logging.debug('Assigning domains to fields')
    management.AssignDomainToField('IMPOUNDMENT', 'WATERSURFACEELEVATIONUOM', 'GSIP_LengthUOM')
    management.AssignDomainToField('IMPOUNDMENT', 'IMPOUNDMENTTYPE', 'ImpoundmentType')
    management.AssignDomainToField('CONTROLMONUMENTPOINT', 'COLLECTIONHORIZONTALDATUM', 'HorizontalDatumType')
    management.AssignDomainToField('CONTROLMONUMENTPOINT', 'COLLECTIONVERTICALDATUM', 'VerticalDatumType')
    management.AssignDomainToField('CONTROLMONUMENTPOINT', 'XLOCATIONUOM', 'GSIP_LengthUOM')
    management.AssignDomainToField('CONTROLMONUMENTPOINT', 'YLOCATIONUOM', 'GSIP_LengthUOM')
    management.AssignDomainToField('CONTROLMONUMENTPOINT', 'ZLOCATIONUOM', 'GSIP_LengthUOM')
    management.AssignDomainToField('NATRESACQUISITIONBOUNDARY_LINE', 'DISTANCEUOM', 'GSIP_LengthUOM')
    management.AssignDomainToField('NATRESACQUISITIONBOUNDARY_LINE', 'PRIVATESURVEY', 'Bool_YN')
    management.AssignDomainToField('DOCKSANDWHARFS_AREA', 'NATUREOFCONSTRUCTION', 'ConstructionMaterialType')
    management.AssignDomainToField('DOCKSANDWHARFS_AREA', 'TYPEOFDOCK', 'TypeOfDock')
    management.AssignDomainToField('DOCKSANDWHARFS_AREA', 'DOCKPURPOSE', 'DockPurposeType')
    management.AssignDomainToField('DOCKSANDWHARFS_AREA', 'ACCESSTYPE', 'DockAccessType')
    management.AssignDomainToField('DOCKSANDWHARFS_PT', 'NATUREOFCONSTRUCTION', 'ConstructionMaterialType')
    management.AssignDomainToField('DOCKSANDWHARFS_PT', 'TYPEOFDOCK', 'TypeOfDock')
    management.AssignDomainToField('DOCKSANDWHARFS_PT', 'DOCKPURPOSE', 'DockPurposeType')
    management.AssignDomainToField('DOCKSANDWHARFS_PT', 'ACCESSTYPE', 'DockAccessType')
    management.AssignDomainToField('PATHWAY', 'PATHWAYTYPE', 'TypeOfPathway')
    management.AssignDomainToField('PATHWAY', 'LENGTHUOM', 'GSIP_LengthUOM')
    management.AssignDomainToField('PATHWAY', 'HEIGHTUOM', 'GSIP_LengthUOM')
    management.AssignDomainToField('TIMBERTRESPASS_AREA', 'TIMBERTYPE', 'TypeOfTimber')
    management.AssignDomainToField('TIMBERTRESPASS_PT', 'TIMBERTYPE', 'TypeOfTimber')
    management.AssignDomainToField('TIMBERTRESPASS_PT', 'COLLECTIONTYPE', 'TypeOfCollection')
    management.AssignDomainToField('TIMBERTRESPASS_PT', 'TREESIZEUOM', 'GSIP_LengthUOM')
    management.AssignDomainToField('FLORAPLANTINGORSEEDING', 'PLANTINGPURPOSETYPE', 'PlantingPurposeType')
    management.AssignDomainToField('FLORAPLANTINGORSEEDING', 'TIMBERTYPE', 'TypeOfTimber')
    management.AssignDomainToField('FLORAPLANTINGORSEEDING', 'ISORNAMENTAL', 'Bool_YN')
    management.AssignDomainToField('FLORAPLANTINGORSEEDING', 'INCLUDESTURF', 'Bool_YN')
    management.AssignDomainToField('SHORELINEMANAGEMENTZONE', 'ZONECLASSIFICATION', 'ShorelineZoneClassificationTyp')
    management.AssignDomainToField('SHORELINEALLOCATIONSIGNS', 'LEFTSIDE', 'ShorelineClassificationColor')
    management.AssignDomainToField('SHORELINEALLOCATIONSIGNS', 'RIGHTSIDE', 'ShorelineClassificationColor')
    management.AssignDomainToField('SHORELINEALLOCATIONSIGNS', 'REPLACE', 'Bool_YN')
    logging.debug('Completed assignment of domains')


def table_wipe():
    fcs = ListFeatureClasses()
    for fc in fcs:
        logging.info('Deleting %s' % fc)
        management.Delete(fc)


def data_prep():
    logging.info('Scrubbing gdb data for enterprise deployment')
    logging.info('  Scrubbing Lanier_Boundary_Line')
    boundary_line = os.path.join(local_workspace, 'Final_Survey.mdb\\Lanier_Boundary_Line')
    fields = 'Private_Survey'
    where_clause = "Private_Survey = 'Private'"
    with da.UpdateCursor(boundary_line, fields, where_clause) as cursor:
        for row in cursor:
            row[0] = 'Y'
            cursor.updateRow(row)

    logging.info('  Scrubbing shorline_zoning')
    shoreline_zoning = os.path.join(local_workspace, 'shoreline_zoning.mdb\\shoreline_zoning')
    zones = {
        'Limited Development': 'limitedDevelopment',
        'Protected': 'protected',
        'Recreation': 'publicRecreation',
        'Restricted': 'prohibitedAccess'
    }
    fields = 'SAZONETYPE'
    where_clause = "SAZONETYPE = 'Limited Development'"

    # Received a "No current record." error without opening an edit session.
    # Thanks for that arcpy
    with da.Editor(os.path.join(local_workspace, 'shoreline_zoning.mdb')) as edit:
        with da.UpdateCursor(shoreline_zoning, fields, where_clause) as cursor:
            for row in cursor:
                old_value = row[0]
                # # for case sensitivity must clear data then repopulate it. I attempted
                # # the following two lines of code, but it still kept the old case sensitivity.
                # # will try using this again if the import into a domain is case sensitive.
                # row[0] = 'clear'
                # cursor.updateRow(row)
                row[0] = zones[old_value]
                cursor.updateRow(row)

    logging.info('  Scrubbing DockDrawing')
    #point_data and management.Addfield function need to be called before I open an edit session
    point_data = os.path.join(local_workspace, 'LanierPermits.mdb\\TimberTrespass\\PointData')
    management.AddField(point_data, 'point_type', 'TEXT', '', '', 50)
    with da.Editor(os.path.join(local_workspace, 'LanierPermits.mdb')) as edit:
        dock_drawing = os.path.join(local_workspace, 'LanierPermits.mdb\\DockDrawing')
        fields = 'Type'
        where_clause = "Type = 'Lease'"
        with da.UpdateCursor(dock_drawing, fields, where_clause) as cursor:
            for row in cursor:
                row[0] = 'public'
                cursor.updateRow(row)

        logging.info('  Scrubbing Pathway')
        path_types = {
            'Improved Walkway with steps': 'improvedPath',
            'Path w/ electric & water line': 'improvedPath',
            "Road  120'": 'road'
        }
        pathway = os.path.join(local_workspace, 'LanierPermits.mdb\\LandBased\\Pathway')
        fields = 'Type'
        where_clause = 'Type is not null'
        with da.UpdateCursor(pathway, fields, where_clause) as cursor:
            for row in cursor:
                if row[0] not in path_types.keys():
                    cursor.deleteRow()
                else:
                    old_value = row[0]
                    row[0] = path_types[old_value]
                    cursor.updateRow(row)
        logging.info('  Scrubbing PointData')
        point_types = {
            'red oak': 'rOak',
            'stump': 'unidentified',
            'Culvert': 'culvert',
            'Nix': None,
            'red maple': 'rMaple',
            'river birch': 'riverBirch',
            'white oak': 'wOak',
            'yellow poplar': 'yPoplar',
            'loblolly pine': 'lobPine',
            'american holly': 'amerHolly',
            'flowering dogwood': 'flowDogwood'
        }
        collection_types = ['culvert', 'irrigation', 'post']
        fields = ['Type', 'point_type']
        where_clause = "[Type] is not Null AND [Type] not in (' ', '')"

        with da.UpdateCursor(point_data, fields, where_clause) as cursor:
            for row in cursor:
                old_value = row[0]
                if row[0] in point_types.keys():
                    row[0] = point_types[old_value]
                if row[0] in collection_types:
                    row[1] = old_value
                    row[0] = None
                cursor.updateRow(row)
        logging.info('  Scrubbing ReplantingSites')
        replanting_sites = os.path.join(local_workspace, 'LanierPermits.mdb\\TimberTrespass\\ReplantingSites')
        fields = "Common"
        where_clause = "[Common] is not Null AND [Common] not in (' ', '')"

        with da.UpdateCursor(replanting_sites, fields, where_clause) as cursor:
            for row in cursor:
                old_value = row[0]
                if row[0] in point_types.keys():
                    row[0] = point_types[old_value]
                    cursor.updateRow(row)
    management.MultipartToSinglepart(
        os.path.join(local_workspace, 'Final_Survey.mdb\\Lake_Lanier_Surface'),
        os.path.join(local_workspace, 'Final_Survey.mdb\\Lake_Lanier_Surface_single'))
    logging.info('Done with enterprise data scrub')


def generate_defaults(fc_path, field, value):
    if field not in ListFields(fc_path):
        management.AddField(fc_path, field, 'TEXT', '', '', 50)
    management.CalculateField(fc_path, field, value)

if __name__ == '__main__':
    main()
