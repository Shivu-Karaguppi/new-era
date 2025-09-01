def multiline_to_singleline(multiline_string):
    # Split the multiline string into lines
    lines = multiline_string.splitlines()
    
    # Strip each line to remove leading and trailing spaces, and join with spaces, adding `\n` at the end of each line
    single_line = " ".join(line.strip() + "\\n" for line in lines if line.strip())

    
    return single_line

# Example usage
multiline_string = """select 
    SUB_CUSTOMER_ID,
    APPLICATION_SUBMITTED_NUM,
    APPLICATION_ID,
    APPLICATION_NUM,
    PLATFORM_APPLICATION_NUM,
    APPLICATION_ENTITY_ID,
    APPLICATION_ENTITY_NUM,
    CARRIER_QUOTE_NUM,
    APPLICATION_SNAPSHOT_NUM,
    CARRIER_CD,
    CARRIER_DESC,
    PRODUCT_CD,
    PRODUCT_DESC,
    PACKAGE_TYPE_CD,
    PACKAGE_TYPE_DESC,
    SUBMISSION_TYPE_CD,
    SUBMISSION_TYPE_DESC,
    QUOTED_PREMIUM_TRAN_CUR,
    QUOTED_PREMIUM_HOME_CUR,
    SUBMISSION_DT,
    ACTIVE_FLAG,
    SUBMISSION_STATUS_CD,
    SUBMISSION_STATUS_DESC,
    SUBMISSION_SOURCE_CD,
    SUBMISSION_SOURCE_DESC,
    POLICY_DURATION_CD,
    POLICY_DURATION_DESC,
    PREMIUM_PAID_DT,
    --CLICKED_BRIDGE_FLAG,
    TEST_FLAG
from adp_core_preprod.dwh.POL_APPLICATION_SUBMITTED_DETAILS 
where customer_id={cust}
    and audit_table_id=(
        SELECT OBJ_ID 
        FROM adp_core_preprod.ADMIN.AUD_OBJECT_MASTER 
        WHERE OBJ_NAME='ODS_RESULTDATA_WF' 
            AND JOB_ID=1
    ) 
    and AUDIT_SYS_ID='11' 
    UNION ALL
select 
    b.SUB_CUSTOMER_ID,
    r.ID,
    b.APPLICATION_ID,
    b.APPLICATION_NUM,
    b.PLATFORM_APPLICATION_NUM,
    b.APPLICATION_ENTITY_ID,
    b.APPLICATION_ENTITY_NUM,
    r.QUOTENUMBER,
    r.RESULTDATASNAPSHOTID,
    MS1.MST_TABLE_BUSINESS_VALUE_CD,
    MS1.MST_TABLE_BUSINESS_VALUE_DESC,
    MS2.MST_TABLE_BUSINESS_VALUE_CD,
    MS2.MST_TABLE_BUSINESS_VALUE_DESC,
    MS3.MST_TABLE_BUSINESS_VALUE_CD,
    MS3.MST_TABLE_BUSINESS_VALUE_DESC,
    MS.MST_TABLE_BUSINESS_VALUE_CD,
    MS.MST_TABLE_BUSINESS_VALUE_DESC,
    r.PREMIUM,
    r.PREMIUM,
    r.DATECREATED,
    CASE 
        WHEN r.ISACTIVE = 1 THEN 'YES' 
        ELSE 'NO' 
    END AS ACTIVE_FLG,
    MS4.MST_TABLE_BUSINESS_VALUE_CD,
    MS4.MST_TABLE_BUSINESS_VALUE_DESC_1,
    MS5.MST_TABLE_BUSINESS_VALUE_CD,
    MS5.MST_TABLE_BUSINESS_VALUE_DESC,
    MS6.MST_TABLE_BUSINESS_VALUE_CD,
    MS6.MST_TABLE_BUSINESS_VALUE_DESC, 
    r.PREMIUMRECIEVEDDATE,
    --CASE WHEN x.IS_CLICKED_BRIDGE =1 THEN 'YES' ELSE 'NO' end as bridge ,
    CASE 
        WHEN (U_entity_num IS NOT NULL 
            OR C_entity_num IS NOT NULL 
            OR UE_entity_num IS NOT NULL 
            OR CE_entity_num IS NOT NULL 
            OR AH_entity_num IS NOT NULL)
        THEN 'YES' 
        ELSE 'NO' 
    END test_flag
from adp_core_preprod.dl{cust}.RESULTDATA R 
JOIN adp_core_preprod.dwh.POL_APPLICATION_DETAILS B 
    ON R.POLICYID = B.APPLICATION_NUM 
    AND B.AUDIT_TABLE_ID IN (
        SELECT OBJ_ID 
        FROM adp_core_preprod.admin.AUD_OBJECT_MASTER 
        WHERE OBJ_NAME='ODS_POLICY_WF' 
            AND JOB_ID=1
    ) 
    --and A.active_flag='YES' 
inner JOIN (
    SELECT * 
    FROM adp_core_preprod.mst.mst_master_value_list
    WHERE MST_TABLE_BUSINESS_NAME ='MST_CARRIER' 
) ms1 
    ON ms1.MST_TABLE_SOURCE_VALUE_CD = R.CARRIER 
    AND ms1.MST_VALUE_LAUGUAGE_CD = 'ENG' 
    AND GETDATE() BETWEEN ms1.EFFECTIVE_START_DT AND ms1.EFFECTIVE_END_DT
inner JOIN (
    SELECT * 
    FROM adp_core_preprod.mst.mst_master_value_list
    WHERE MST_TABLE_BUSINESS_NAME ='MST_PRODUCT' 
) ms2 
    ON ms2.MST_TABLE_SOURCE_VALUE_CD = R.LOB 
    AND ms2.MST_VALUE_LAUGUAGE_CD = 'ENG' 
    AND GETDATE() BETWEEN ms2.EFFECTIVE_START_DT AND ms2.EFFECTIVE_END_DT
left JOIN (
    SELECT * 
    FROM adp_core_preprod.mst.mst_master_value_list
    WHERE MST_TABLE_BUSINESS_NAME ='MST_PACKAGE_TYPE' 
) ms3 
    ON ms3.MST_TABLE_SOURCE_VALUE_CD = R.PACKAGETYPE 
    AND ms3.MST_VALUE_LAUGUAGE_CD = 'ENG' 
    AND GETDATE() BETWEEN ms3.EFFECTIVE_START_DT AND ms3.EFFECTIVE_END_DT
left JOIN (
    SELECT * 
    FROM adp_core_preprod.mst.mst_master_value_list
    WHERE MST_TABLE_BUSINESS_NAME ='MST_RESULT_DATA_TYPE' 
) ms 
    ON ms.MST_TABLE_SOURCE_VALUE_CD = R.TYPE 
    AND ms.MST_VALUE_LAUGUAGE_CD = 'ENG' 
    AND GETDATE() BETWEEN ms.EFFECTIVE_START_DT AND ms.EFFECTIVE_END_DT
left JOIN (
    SELECT * 
    FROM adp_core_preprod.mst.mst_master_value_list
    WHERE MST_TABLE_BUSINESS_NAME ='MST_RESULT_STATUS_TYPE' 
) ms4 
    ON ms4.MST_TABLE_SOURCE_VALUE_CD = R.STATUS 
    AND ms4.MST_VALUE_LAUGUAGE_CD = 'ENG' 
    AND GETDATE() BETWEEN ms4.EFFECTIVE_START_DT AND ms4.EFFECTIVE_END_DT
left JOIN (
    SELECT * 
    FROM adp_core_preprod.mst.mst_master_value_list
    WHERE MST_TABLE_BUSINESS_NAME ='MST_RESULT_SOURCE_TYPE' 
) ms5 
    ON ms5.MST_TABLE_SOURCE_VALUE_CD = R.SOURCE 
    AND ms5.MST_VALUE_LAUGUAGE_CD = 'ENG' 
    AND GETDATE() BETWEEN ms5.EFFECTIVE_START_DT AND ms5.EFFECTIVE_END_DT
left JOIN (
    SELECT * 
    FROM adp_core_preprod.mst.mst_master_value_list
    WHERE MST_TABLE_BUSINESS_NAME ='MST_POLICY_DURATION_TYPE' 
) ms6 
    ON ms6.MST_TABLE_SOURCE_VALUE_CD = R.TERM 
    AND ms6.MST_VALUE_LAUGUAGE_CD = 'ENG' 
    AND GETDATE() BETWEEN ms6.EFFECTIVE_START_DT AND ms6.EFFECTIVE_END_DT
LEFT JOIN adp_core_preprod.dl{cust}.POLICY A 
    ON R.POLICYID = A.ID 
    AND A.active_flag = 'YES'
-- LEFT JOIN adp_core_preprod.dl14.USERS C ON A.CREATEDBYUSERID=C.ID and C.active_flag='YES'
LEFT JOIN (
    select distinct e.entity_num as U_entity_num 
    from adp_core_preprod.dwh.ENT_ENTITY_DETAILS E 
    join adp_core_preprod.admin.cfg_configuration_details cd 
        on upper(e.entity_full_nm) like upper(cd.config_desc)
        and cd.config_entity_name = 'Test Data - Consumer and Agent Name' 
        and cd.customer_id = 0 
    where e.audit_table_id = (
        select obj_id 
        from adp_core_preprod.admin.aud_object_master 
        where obj_name='ODS_USERS_WF' 
            and job_id = 1
    ) 
    and e.customer_id = {cust}
) ED 
    ON ED.U_entity_num = A.CREATEDBYUSERID 
--and ED.CUSTOMER_ID=14 AND ED.AUDIT_TABLE_ID = (SELECT OBJ_ID FROM AUD_OBJECT_MASTER WHERE OBJ_NAME='ODS_USERS_WF' AND JOB_ID=1)
LEFT JOIN (
    select distinct e1.entity_num as C_entity_num 
    from adp_core_preprod.dwh.ENT_ENTITY_DETAILS e1 
    join adp_core_preprod.admin.cfg_configuration_details cd 
        on upper(e1.entity_full_nm) like upper(cd.config_desc)
        and cd.config_entity_name = 'Test Data - Consumer and Agent Name' 
        and cd.customer_id = 0 
    where e1.audit_table_id = (
        select obj_id 
        from adp_core_preprod.admin.aud_object_master 
        where obj_name='ODS_CONSUMER_WF' 
            and job_id = 1
    ) 
    and e1.customer_id = {cust}
) ED1 
    ON ED1.C_entity_num = A.CONSUMERID  
--and ED1.CUSTOMER_ID=38 AND ED.AUDIT_TABLE_ID = (SELECT OBJ_ID FROM AUD_OBJECT_MASTER WHERE OBJ_NAME='ODS_CONSUMER_WF' AND JOB_ID=1)
LEFT JOIN (
    select distinct c.entity_num as UE_entity_num 
    from adp_core_preprod.dwh.ENT_ENTITY_CONTACT c 
    join adp_core_preprod.admin.cfg_configuration_details cd 
        on upper(c.entity_email) like upper(cd.config_desc)
        and cd.config_entity_name = 'Test Data- Agent Email' 
        and cd.customer_id = 0 
    where c.customer_id = {cust} 
        and c.audit_table_id = (
            select obj_id 
            from adp_core_preprod.admin.aud_object_master 
            where obj_name='ODS_USERS_WF' 
                and job_id = 1
        )
) EC 
    ON EC.UE_entity_num = A.CREATEDBYUSERID  
LEFT JOIN (
    select distinct c1.entity_num as CE_entity_num 
    from adp_core_preprod.dwh.ENT_ENTITY_CONTACT c1 
    join adp_core_preprod.admin.cfg_configuration_details cd 
        on upper(c1.entity_email) like upper(cd.config_desc)
        and cd.config_entity_name = 'Test Data - Consumer email' 
        and cd.customer_id = 0 
    where c1.customer_id = {cust}
        and c1.audit_table_id = (
            select obj_id 
            from adp_core_preprod.admin.aud_object_master 
            where obj_name='ODS_CONSUMER_WF' 
                and job_id = 1
        )
) EC1 
    ON EC1.CE_entity_num = A.CONSUMERID
LEFT JOIN (
    select distinct AH.agent_entity_num as AH_entity_num 
    from adp_core_preprod.dwh.AGT_HIERARCHY ah  
    join adp_core_preprod.admin.cfg_configuration_details cd 
        on upper(ah.HIERARCHY_NM) like upper(cd.config_desc)
        and cd.config_entity_name = 'Test Data - Hierarchy Name' 
        and cd.customer_id = 0 
    where ah.customer_id = {cust}
) AH 
    ON AH.AH_entity_num = A.CREATEDBYUSERID
where R.active_flag = 'YES'
    and b.CUSTOMER_ID = {cust} 
    AND b.AUDIT_SYS_ID = '11'"""

single_line_string = multiline_to_singleline(multiline_string)
print(single_line_string)
