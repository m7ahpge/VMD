# File name: batch_update_from_Oracle.py
# Author: Siyu Fan (S3F1)
# Created: May/06/2020
# Last modified: Jun/05/2020
# Modified by : Manisha (M7AH)
# Description: Retrieve data entries by Salesforce team about Parcel and push data into AGOL.
# TODO: 1. QA TESTING

import datetime
import os
import traceback

import arcpy

try:
    from EVM_sync import EVMUtils
except Exception as e:
    # this_dir = os.path.split(os.path.realpath(__file__))[0]
    # import sys
    # sys.path.append(this_dir)
    import sys
    sys.path.append(r'C:\Users\m7ah\Documents\PythonScripts_PROD\Git_repo_pge\gis-geomart-evm-master\EVM_sync')
    from EVM_sync.EVM_sync import EVMUtils


class AGOL_Updater(EVMUtils):
    def __init__(self, caller, config):
        super(AGOL_Updater, self).__init__(caller, config)
        self.ID_FIELD = 'AUTO_ID'
        self.DATA_TABLE = 'EVMGIS.VMD_TESTING'
        self.VALIDATION_FIELDS = []
        self.HEADER = ['AUTO_ID','TREESTATUS','SCONTCODE','SDIVCODE','SLOCALID','SCOMMENT','CIRCUITNAME','SWORKBY','WORKEDASPRESCRIBED','ACTUALQTY','ACTUALTRIMCODE','ACTUALCLEARANCE',
                       'ITREEADDED','SWORKREQ', 'IWRSORT','PRESCRIBEDQTY', 'STREECODE','NHEIGHT','NDBH','PRESCRIBEDCLEARANCE','STRIMCODE','PRIORITY','TREECOMMENT','SRXCOMMENTS','BDEADORDYING','WOODMGMTNUMBER']
      #                 'SMWS','SMWSDOCNUM','BTREEWIRE','BSRA','SACCOUNTTYPE','ITREERECSID','IWRTREERECSID','SINSPCOMP','SINSP','SREGION','WORKBUNDLE','SPROJECTNAME','IPROJID','SSOURCEDEV','ISSDROUTE','IROUTENUM',
       #                'STREETNO','STREET','CITY','SCUSTNAME','SCUSTPHONE','SCUSTPHONE2','LOCATIONCOMMENTS','SDIRECTIONS','ALERTS','SAPN','LANDOWNER','TREELAT','TREELONG','RFID_NO','TREE_WORKVERIFICATION_ID']
        self.SDE_CONN = self.config['workspace']
        self.RECIPIENT = ['m7ah@pge.com']

    def get_data_from_sde(self, table_name: str, fields: list, sql_where: str = None, sorted_by: str = None) -> list:
        conn = arcpy.ArcSDESQLExecute(self.SDE_CONN)
        sql = f"SELECT {','.join(fields)} FROM {table_name}"
        if sql_where:
            sql += f" WHERE {sql_where}"
        if sorted_by:
            sql += f" ORDER BY {sorted_by}"
        try:
            _sql_results = conn.execute(sql)
            del conn
        except Exception as e:
            self.logger.error(f"Exception during query. Query: {sql}")
            return []
        if type(_sql_results) is bool:
            if _sql_results:
                self.logger.warning(f"No results returned from query. Query {sql}.")
            else:
                self.logger.error(f"Query error. Query: {sql}.")
            return []

        return _sql_results


    def validate_data_value(self, field_type, field_length, field, value) -> object:
        if not value:
            return value

        if field_type == "DATE":
            return self.validate_datetime_str(value, self.DATETIME_FORMAT)
        elif field_type == "INTEGER":
            try:
                i = int(value)
                return i
            except ValueError as ve:
                self.logger.error(f'Value [{value}] could not be converted to Integer. Error: {ve}')
        elif field_type == "FLOAT":
            try:
                f = float(value)
                return f
            except ValueError as ve:
                self.logger.error(f'Value [{value}] could not be converted to Float. Error: {ve}')
        else:
            if len(value) > field_length:
                self.logger.error(f'Value [{value}] exceeds the field length of {field}. ')
            else:
                return value
        return None


    def validate_datetime_str(self, datetime_str, format_str):
        if not datetime_str or len(datetime_str) == 0:
            return None
        try:
            dt = datetime.datetime.strptime(datetime_str, format_str)
        except Exception as e:
            self.logger.error(f"Exception during datetime conversion. Error: {e}")
            return None
        return dt

    def update_agol(self, results: dict, table_lyr_mapping: dict):
        if not results or not table_lyr_mapping:
            return

        updated_oid_list, failed_oid_list = [], []

        for table_name, value_dict in results.items():
            if table_name not in table_lyr_mapping:
                continue
            item_id, lyr_index = table_lyr_mapping[table_name]
            lyr = self.get_gis().content.get(item_id).layers[lyr_index]
            self.logger.info(f"Updating {lyr.properties['name']} layer on AGOL...")
            n = len(value_dict)
            for index, key in enumerate(value_dict, start=1):
                (id_field, id_value) = key
                value_pairs = value_dict[key]
                where = f"{id_field}='{id_value}'"
                features_to_update = []
                counter, failed = 0, 0
                try:
                    features = lyr.query(where,
                                         return_geometry=False
                                         ).features

                    if len(features) == 0:
                        self.logger.warning(f"Batch {index} of {n}. No feature on AGOL matching [{id_field}='{id_value}'].")
                        failed_oid_list.append(f"{id_field}='{id_value}' AND ACTION='UPDATE'")
                        continue

                    for f in features:
                        flag = False
                        for field, value in value_pairs:
                            try:
                                if field == self.ID_FIELD:
                                    continue
                                if f.attributes[field] != value:
                                    f.attributes[field] = value
                                    flag = True
                            except Exception as e:
                                self.logger.error(f"Exception during feature attribute update. Error: {e}")
                        if flag:
                            features_to_update.append(f)

                    if features_to_update:
#                        self.update_editing_info(lyr, features_to_update)
                        update_result = lyr.edit_features(updates=features_to_update, rollback_on_failure=True, use_global_ids=True)
                        updated_at = datetime.datetime.now().strftime(self.DATETIME_FORMAT)
                        for r in update_result['updateResults']:
                            if not r['success']:
                                failed += 1
                                self.logger.error(f"Failed to update [{where}] and AGOL OBJECTID='{r['objectId']}'. {r}")
                            else:
                                counter += 1
                        if failed == 0:
                            updated_oid_list.append((f"{id_field}='{id_value}' AND ACTION='UPDATE'", f"TO_DATE('{updated_at}', '{self.ORACLE_DATETIME_FORMAT}')"))
                            self.logger.info(f"Batch {index} of {n}. {counter} of {len(features_to_update)} AGOL features updated. {failed} not updated on AGOL. [{id_field}='{id_value}'].")
                        else:
                            failed_oid_list.append(f"{id_field}='{id_value}' AND ACTION='UPDATE'")
                            self.logger.error(f"Batch {index}. Not all features on AGOL matching [{id_field}='{id_value}'] of SDE record [{where}] were updated. {failed} failed. Please check log for detail.")
                    else:
                        updated_oid_list.append((f"{id_field}='{id_value}' AND ACTION='UPDATE'", f"TO_DATE('{datetime.datetime.now().strftime(self.DATETIME_FORMAT)}', '{self.ORACLE_DATETIME_FORMAT}')"))
                        self.logger.warning(f"Batch {index} of {n}. No need to update on AGOL matching [{where}].")
                except Exception as e:
                    failed_oid_list.append(f"{id_field}='{id_value}' AND ACTION='UPDATE'")
                    self.logger.info(f"Exception for batch {index} of {n}. [{where}]. Error: {e}.")
                    self.logger.error(f"Traceback: {traceback.format_exc()}")

        return updated_oid_list, failed_oid_list

    def build_features_from_rows(self, data_rows,layer_mapping ):
        feature_dict = {}
        for (table_name, id_name, id_value, field, value) in data_rows:
            try:
                _, _, _, ft, fl = layer_mapping[table_name][field]
                if table_name not in feature_dict:
                    feature_dict[table_name] = {}
                key = (id_name, id_value)
                if key not in feature_dict[table_name]:
                    feature_dict[table_name][key] = []
                validated_value = self.validate_data_value(ft, fl, field, value)
                if ft == 'DATE':
                    validated_value = validated_value.timestamp() * 1000
                feature_dict[table_name][key].append((field, validated_value))
            except Exception as e:
                self.logger.error(f"Error: {e}.")
                self.logger.error(f"Traceback: {traceback.format_exc()}")
        return feature_dict


if __name__ == '__main__':
    this_dir = os.path.split(os.path.realpath(__file__))[0]
    config = os.path.join(this_dir, 'config_sync_QA_local_v8.json')
    runner = AGOL_Updater('AGOL_Updater', config)

    # Load layer mapping and field mapping rules.
    layer_mapping = {'vmd': ['19161a3a4c2b48edaa78c8b54d354495', 0]}
    email_content = ''
    # Query for valid values and update them on AGOL.
    # data_dict = {}
    update_where = "ACTION= 'UPDATE'"
    sql_results = runner.get_data_from_sde(runner.DATA_TABLE, runner.HEADER, sql_where=update_where, sorted_by=runner.ID_FIELD)
    feature_dict = {'vmd': {}}
    for row in sql_results:
        field_value_pairs = zip(runner.HEADER, row)
        for field, value in field_value_pairs:
            if ('AUTO_ID', row[0]) not in feature_dict['vmd']:
                feature_dict['vmd'][('AUTO_ID', row[0])] = []
#            if field == 'ACCESS_':
#               feature_dict['evm_parcel'][('UNIQUE_PARCEL_ID', row[0])].append(('ACCESS', value))
            else:
                feature_dict['vmd'][('AUTO_ID', row[0])].append((field, value))
#           if field == 'COMMENTS':
#               feature_dict['evm_parcel'][('UNIQUE_PARCEL_ID', row[0])].append(('LCE_NOTES', value))
#            else:
#               feature_dict['evm_parcel'][('UNIQUE_PARCEL_ID', row[0])].append((field, value))

    if not len(feature_dict['vmd']):
        runner.logger.info("Empty query result.")
    oid_and_updated_list, oid_failed_list = runner.update_agol(feature_dict, layer_mapping)
    runner.logger.info(f"Updating Oracle table [{runner.DATA_TABLE}] for update timestamp...")

    # Verify if update on AGOL is successful.

    email_content += f"{len(oid_and_updated_list)} updates are validated. {len(oid_failed_list)} updates are not validated."

    runner.send_notification_emails(f"UPDATE REPORT at {runner.date} - {runner.time}",
                                    runner.RECIPIENT,
                                    email_content)

