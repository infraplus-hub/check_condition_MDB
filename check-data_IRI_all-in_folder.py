import pyodbc
import csv
import psycopg2
import pandas as pd
import os,sys
import shutil
from pathlib import Path
#### เตรียมตัวก่อนใช้งาน ####------------------------------------------------------------------------------------------------
# 1. ที่อยู่ไฟล์จะต้องอยู่ในโฟลเดอร์ที่ชื่อว่า 'survey_data' และตามดัววันที่ '20220614'
# 2. ชื่อไฟล์ .mdb จะใช้ตัวระบุลงท้ายด้วย '_edit'
# 3. ชื่อไฟล์และชื่อตารางในไฟล์จะต้องสัมพันธ์กัน
# 4. เปลี่ยนการเชื่อมต่อ database ของแต่ละโครงการ
# 5. กรณีมีงานก่อนสร้างให้ใช้คำว่า 'construction'
# 6. กรณีต้องนำเข้าข้อมูลเข้าระบบ roadnet หรือ อื่นที่ใช้ max_id ให้ไปแก้ code survey และ survey_point_local
# 7. อย่าลืม !!! set directory ไฟล์ .bat เพราะแต่ละโครงการโครงสร้างการเก็บรูปไม่เหมือนกัน
#----------------------------------------------------------------------------------------------------------------------

'''drop table access_key,access_pic,access_valuelaser,access_distress_pic,data_suvey,gps_lost,
survey,survey_point_local,survey_point,survey_image,data_pave_suvey,gps_lost_pave,survey_pave'''

###### File Directory Management #####
ext = ('.mdb')
date_survey =  '20220608'    #str(input('วันสำรวจ(20220804)='))
path_direc = r'D:\sourc_code\code_IRI\data_test\survey_data'
path = r'%s\%s' %(path_direc,date_survey)
file_dir = Path(path).glob('*.mdb')

#### create mkdir output ######################
path_out = r'%s\output' % (path)
if not os.path.exists(path_out):
    os.makedirs(path_out)
else:
    shutil.rmtree(path_out)
    os.makedirs(path_out)

for file in file_dir:
    path_mdb = str(file)
   ## processing pyodbc ########################
    pyodbc.lowercase = False
    conPG = pyodbc.connect(
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};" +
        r"Dbq=%s;" % (path_mdb))
    ## split string ########################
    a1,a2 = path_mdb.split('survey_data')
    b1, b2, b3 = a2.split('\\')
    a, c2 = b3.split('_edit')
    #### create mkdir ######################
    path_dir = '%s\process\%s' % (path, a)
    if not os.path.exists(path_dir):
        os.makedirs(path_dir)
    else:
        shutil.rmtree(path_dir)
        os.makedirs(path_dir)
    ####################### Fetching Data access_valuelaser  ############################################################################################################
    cre_valuelaser = '''SELECT a.CHAINAGE as chainage, LONGITUDE as lon, LATITUDE as lat,
            RWP_IRI AS iri_right, LWP_IRI AS iri_left,  (((RWP_IRI)+(LWP_IRI))/2) as iri  ,LANE_IRI AS iri_lane,
            RUT_EDG_SE  AS rutt_right, RUT_CTR_SE AS rutt_left, RUT_SE AS rutting,
            LANE_MPD AS texture, ((LANE_MPD)*0.8)+0.008 as etd_texture,
            '%s' as file_name
            FROM (((GPS_Processed_%s AS a)
            LEFT JOIN Profiler_IRI_%s as b on a.CHAINAGE = b.CHAINAGE)
            left join TPL_Processed_%s as c on a.CHAINAGE = c.CHAINAGE)
            left join Profiler_MPD_%s as d on a.CHAINAGE = d.CHAINAGE''' % (a, a, a, a, a)
    cur = conPG.cursor()
    sql = cre_valuelaser
    cur.execute(sql)
    with open('%s\\process\\%s\\access_valuelaser.csv' % (path, a), 'w', newline='') as f:
        writer = csv.writer(f)
        for row in cur.fetchall():
            writer.writerow(row)
    cur.close()
    folders ='%s\\process\\%s\\access_valuelaser.csv' %(path,a)
    ####################### Fetching Data access_key  ############################################################################################################
    cre_key = '''SELECT CHAINAGE_START as event_str, CHAINAGE_END as event_end, EVENT AS event_num,
            SWITCH_GROUP as event_type, EVENT_DESC as event_name, link_id, section_id, km_start, km_end, length,
            lane_no, survey_date,LATITUDE_START as lat_str, LATITUDE_END as lat_end, LONGITUDE_START as lon_str,
            LONGITUDE_END as lon_end, '%s' as name_key,ramp_id
            from KeyCode_Raw_%s''' % (a,a)
    cur = conPG.cursor()
    sql = cre_key
    cur.execute(sql)
    with open('%s\\process\\%s\\access_key.csv' % (path, a), 'w', newline='') as f:
        writer = csv.writer(f)
        for row in cur.fetchall():
            writer.writerow(row)
    cur.close()
    ####################### Fetching Data access_pic  ############################################################################################################
    cre_pic = '''select CHAINAGE as chainage_pic, FRAME as  frame_number , '%s' as name_pic
            from Video_Processed_%s_1''' % (a,a)
    cur = conPG.cursor()
    sql = cre_pic
    cur.execute(sql)
    with open('%s\\process\\%s\\access_pic.csv' % (path, a), 'w', newline='') as f:
        writer = csv.writer(f)
        for row in cur.fetchall():
            writer.writerow(row)
    cur.close()
    ####################### Fetching Data access_pave  ############################################################################################################
    cre_pave = '''select CHAINAGE as chainage_pic, FRAME as  frame_number , X as lon, Y as lat,'%s' as name_pave
            from Video_Processed_%s_2''' % (a,a)
    cur = conPG.cursor()
    sql = cre_pave
    cur.execute(sql)
    with open('%s\\process\\%s\\access_distress_pic.csv' % (path, a), 'w', newline='') as f:
        writer = csv.writer(f)
        for row in cur.fetchall():
            writer.writerow(row)
    cur.close()
print('STEP1 processing MDB to CSV successfully')

conPG = psycopg2.connect(
    host="localhost",
    database="roadnet",
    user="postgres",
    password="123456",
    port="5432"
)
print("STEP2 Connect DB successfully")


##STEP_1  'Create Table access'####################################################################################################################################
table_valuelaser = '''
CREATE TABLE access_valuelaser
		(
		  chainage real, lon real, lat real, iri_right text, iri_left text,
		  iri text, iri_lane numeric(8,2), rutt_right numeric(8,2), rutt_left numeric(8,2),
		  rutting numeric(8,2), texture numeric(8,2), etd_texture numeric(8,2), file_name text
		)
	'''
cur_valuelaser = conPG.cursor()
cur_valuelaser.execute(table_valuelaser)

table_key = '''
CREATE TABLE access_key
		(
		  event_str real, event_end real, event_num char(2), event_type char(20), event_name char(20),
		  link_id char(20), section_id char(50), km_start char(50), km_end char(50),  length char(100),
		  lane_no char(50), survey_date char(50),
		  lat_str real, lat_end real, lon_str real, lon_end real,name_key char(20),ramp_id text
		)
    '''
cur_key = conPG.cursor()
cur_key.execute(table_key)

table_pic = '''
CREATE TABLE access_pic
		(
		  chainage_pic real,
		  frame_number char(20),
		  name_pic char(20)
		  )
		  '''
cur_pic = conPG.cursor()
cur_pic.execute(table_pic)

table_pave = '''
CREATE TABLE access_distress_pic
  (
    chainage_pic real,
    frame_number char(20),
    lon double precision , 
    lat double precision,
    name_pave char(20)
    )
'''
cur_pave = conPG.cursor()
cur_pave.execute(table_pave)
conPG.commit()
print('STEP3:create table postgresql successfully')

path_processing =  r'%s\%s\process' %(path_direc,date_survey)
print(path_processing)
file_csv = Path(path_processing).glob('*')

for doc in file_csv:
    path_mdb = str(doc)
    #STEP_2  'insert into data to table_access'########################################
    insert_table = '''
            COPY access_key
            FROM '%s\\access_key.csv'
            DELIMITER ',' CSV;
            COPY access_pic
            FROM '%s\\access_pic.csv'
            DELIMITER ',' CSV;
            COPY access_valuelaser
            FROM '%s\\access_valuelaser.csv'
            DELIMITER ',' CSV;
            COPY access_distress_pic
            FROM '%s\\access_distress_pic.csv'
            DELIMITER ',' CSV; ''' % (path_mdb,path_mdb,path_mdb,path_mdb)
    cur_step1 = conPG.cursor()
    cur_step1.execute(insert_table)
    conPG.commit()
print("STEP4:insert CSV successfully")

##STEP_3.1  create table 'data_suvey'####################################################################################################################################
merge_csv = '''
create table data_suvey as --edit 220119 /date
		select a.*, status, status_type
		from
				(
					select --chainage, link_id,
					(replace(km_start, '+', ''))::int+ chainage-event_str as km2,
					replace(survey_date, ' ','')::date as date, a.*, b.*,
					st_setsrid(st_makepoint(lon::real ,lat::real),4326) as the_geom
					from access_key a,
					(select *
					from
						(
							select *, ((iri_right::double precision + iri_left::double precision)/2)::text as iri
							from
							(
								SELECT chainage, lon, lat,
								case when iri_right::real > 8 then (random_between(7.00, 8.00))::text else iri_right end iri_right,
								case when iri_left::real > 8 then (random_between(7.00, 8.00))::text else iri_left end iri_left,
								iri as iri_old, iri_lane, rutt_right, rutt_left, rutting, texture, etd_texture, file_name
								FROM access_valuelaser
							) foo
						) a
					left join access_pic b on  chainage = chainage_pic::int and file_name = name_pic ) b
					where chainage between (event_str::int) and (event_end::int) and replace(event_type, ' ', '') = 'pavetype.'
					--and chainage < 31350
					order by chainage, event_str
				) a
		inner join
				(
				select *
				from
				(
					select *, case when status = 'มีGPSlost' and count < 11 then 'ดำเนินการต่อ'
					when status = 'มีGPSlost' and count > 10 then '***ไม่ดำเนินการต่อ' else 'ดำเนินการต่อ' end status_type
					from
					(	--เชค ข้อมูล ต้องเท่ากับจำนวน Link_id ใน Key_code
						select file_name, replace(survey_date, ' ','')::date as date,  link_id, count(*) as count,
						'มีGPSlost'::text status
						from (
							select a.*, b.*
							from access_key a,
							(select * from access_valuelaser left join access_pic b on  chainage = chainage_pic::int and file_name = name_pic) b
							where chainage between (event_str-5) and (event_end+5) and replace(event_type, ' ', '') = 'pavetype.'
							and split_part((chainage/ CAST(25 AS float))::text, '.', 2) = '' and file_name = name_pic and file_name = name_key
							) foo
						where (lon = 0 or lat = 0) and link_id is not null
						group by file_name, link_id, replace(survey_date, ' ','')::date
						union
						select a.*
						from
						(
							select file_name, replace(survey_date, ' ','')::date as date,  link_id, count(*) as count,
							'ไม่มีGPSlost'::text status
							from (
								select a.*, b.*
								from access_key a,
								(select * from access_valuelaser left join access_pic b on  chainage = chainage_pic::int and file_name = name_pic ) b
								where chainage between (event_str-5) and (event_end+5) and replace(event_type, ' ', '') = 'pavetype.'
								and split_part((chainage/ CAST(25 AS float))::text, '.', 2) = '' and file_name = name_pic and file_name = name_key
								) foo
							where (lon != 0 or lat != 0) --and replace(survey_date, ' ','') > '2021-01-18'
							group by file_name, link_id, replace(survey_date, ' ','')::date
						) a
						left join
						(
							select file_name, replace(survey_date, ' ','')::date as date,  link_id, count(*) as count,
							'มีGPSlost'::text status
							from (
								select a.*, b.*
								from access_key a,
								(select * from access_valuelaser left join access_pic b on  chainage = chainage_pic::int and file_name = name_pic) b
								where chainage between (event_str-5) and (event_end+5) and replace(event_type, ' ', '') = 'pavetype.'
								and split_part((chainage/ CAST(25 AS float))::text, '.', 2) = '' and file_name = name_key and file_name = name_pic
								) foo
							where (lon = 0 or lat = 0)
							group by file_name, link_id, replace(survey_date, ' ','')::date
						) b on a.link_id = b.link_id and a.file_name = b.file_name
						where b.link_id is null and a.link_id is not null
						order by link_id
					) foo
				) foo
				where status_type = '***ไม่ดำเนินการต่อ' or status_type = 'ดำเนินการต่อ'
		) b on a.link_id = b.link_id and a.file_name = b.file_name
		order by link_id,file_name,chainage'''
cur_step21 = conPG.cursor()
cur_step21.execute(merge_csv)
print("STEP5:create table data_suvey successfully")
print('')
conPG.commit()
print('___REPORT_PROBLEM-GPS____________________________________________________________________')
##STEP_3.2  chcek_LAT_LON//STR-END ####################################################################################################################################
str_end = '''
		SELECT name_key,event_str, event_end,link_id, km_start::text, km_end::text,lane_no::text, lat_str::numeric(8,4),
        lat_end::numeric(8,4), lon_str::numeric(8,4), lon_end::numeric(8,4)
	    FROM public.access_key
        where link_id is not null and (lon_str = 0 or lon_end = 0  or lat_str = 0 or lat_end = 0)'''
cur_step30 = conPG.cursor()
cur_step30.execute(str_end)
conPG.commit()
my_table = pd.read_sql(str_end, conPG)
pd.options.display.max_columns = None
pd.options.display.width=None
print('############### ตรวจสอบ KeyCode กรณีมี link_id ที่ lon_str,lon_end,lat_str,lat_end เป็น 0 ##################' )
print(my_table)
print('______________________________________________________________________')


##STEP_3.2-1  chcek Link_ซ้ำ หรือไม่ ####################################################################################################################################
str_end301 = '''
		select link_id,cc
from
	(select link_id,count(link_id) as cc
	from
		(select event_str,event_end,link_id
		from data_suvey
		group by event_str,event_end,link_id
		order by event_str) foo
	group by link_id)foo
where cc > 1'''
cur_step301 = conPG.cursor()
cur_step301.execute(str_end301)
conPG.commit()
my_table = pd.read_sql(str_end301, conPG)
pd.options.display.max_columns = None
pd.options.display.width=None
print('############### ตรวจสอบ KeyCode ว่ามี LINK_ID ซ้ำกันหรือไม่ ??? ##################' )
print(my_table)
print('______________________________________________________________________')


##STEP_3.2-1  chcek ramp_id กับ link_id  ####################################################################################################################################
str_end302 = '''
		select file_name,link_id,ramp_id,
case when substring(split_part(link_id,'C',1),1,1) = '1' and ramp_id is null then 'ถูกแล้ว'
	 when substring(split_part(link_id,'C',1),1,1) = '1' and ramp_id is not null then 'ลืมกรอก ramp_id'
	 when substring(split_part(link_id,'C',1),1,1) != '1' and ramp_id is null then 'ลืมกรอก ramp_id'
	 when substring(split_part(link_id,'C',1),1,1) != '1' and ramp_id is not null then 'ถูกแล้ว' end as ramp_stutus
from data_suvey
group by link_id,ramp_id,file_name
order by file_name,link_id '''
cur_step302 = conPG.cursor()
cur_step302.execute(str_end302)
conPG.commit()
my_table = pd.read_sql(str_end302, conPG)
pd.options.display.max_columns = None
pd.options.display.width=None
print('############### ตรวจสอบ ramp_id กับ link_id ว่าลืมกรอกหรือไม่ ##################' )
print(my_table)
print('______________________________________________________________________')


##STEP_3.3  chcek_gps_loss ####################################################################################################################################
querry_csv = '''
		select link_id,min(chainage),max(chainage),count(the_geom)
        from
        (
            select link_id,chainage,cn,the_geom,lon,lat
            from
                (
                    select link_id,chainage,right((cast(chainage as varchar(10))),2) as cn,
                    st_setsrid(st_makepoint(lon::real ,lat::real),4326) as the_geom,
                    lon,lat
                    from data_suvey
                    order by chainage
                )foo
            where cn = '00' or cn = '25' or cn = '50' or cn = '75'
        ) foo
        where lon = 0 or lat =0
        GROUP BY link_id,the_geom
        HAVING COUNT(the_geom) > 10
        ORDER BY min,COUNT(the_geom) DESC'''
cur_step31 = conPG.cursor()
cur_step31.execute(querry_csv)
conPG.commit()
my_table = pd.read_sql(querry_csv, conPG)
pd.options.display.max_columns = None
pd.options.display.width=None
print('############### ระวังงงงง !!!! มี link_id ที่มี GPS lost มากกว่า 10 ควรตรวจสอบผลลัพธ์หลังจากประมวลผล:##################' )
print(my_table)
print('______________________________________________________________________')

#STEP_3.4  chcek chainage ที่มี GPS ซ้ำ ####################################################################################################################################
c_gps = '''
        select link_id,min(chainage),max(chainage),count(the_geom)
        from
        (
            select link_id,chainage,cn,the_geom,lon,lat
            from
                (
                    select link_id,chainage,right((cast(chainage as varchar(10))),2) as cn,
                    st_setsrid(st_makepoint(lon::real ,lat::real),4326) as the_geom,
                    lon,lat
                    from data_suvey
                    order by chainage
                )foo
            where cn = '00' or cn = '25' or cn = '50' or cn = '75'
        ) foo
        where lon != 0 or lat !=0
        GROUP BY link_id,the_geom
        HAVING COUNT(the_geom) > 1
        ORDER BY min,COUNT(the_geom) DESC
'''
cur_step36 = conPG.cursor()
cur_step36.execute(c_gps)
conPG.commit()
my_table5 = pd.read_sql(c_gps, conPG)
pd.options.display.max_columns = None
pd.options.display.width=None
print('###############chcek chainage ที่มี GPS ซ้ำ##################' )
print(my_table5)
print('______________________________________________________________________')


##STEP_3.2  chcek_gps_loss ####################################################################################################################################
gps_loss = '''create table gps_lost as
		select a.*, b.chainage, min-6 as new_p_min, max+6 as new_p_max
		from
		(
			--max min ช่วง GPS ที่เริ่มหาย
			select min(chainage) as min, max(chainage) as max, count(*) as c_p, count(*)*5 as meter,
			link_id, date, grp2
			from
			(
				select chainage, lat, lon, event_str, event_end, link_id, date,
				row_number() OVER (partition by lat, lon, link_id order by  chainage) as grp1,
				row_number() over (partition by date, link_id order by  chainage)  -   --(minus)
				row_number() OVER (partition by lat, lon, link_id order by  chainage) as grp2
				from data_suvey
				order by chainage
			) foo
			where (lon = 0 or lat = 0)
			group by grp2, link_id, date, grp2
			order by min
		) a,
		(
		select chainage, lat, lon, event_str, event_end, link_id, date
		from data_suvey
		where split_part((chainage/ CAST(25 AS float))::text, '.', 2) = '' and (lon = 0 or lat = 0)
		and status = 'มีGPSlost'
		) b
		where chainage between min and max
		group by min, max, c_p, meter, a.link_id, a.date, grp2, b.chainage, new_p_min, new_p_max
		order by min'''
cur_step22 = conPG.cursor()
cur_step22.execute(gps_loss)
conPG.commit()

##STEP_4.1  update position str and end (even) : STR  ####################################################################################################################################
step41 = '''
update data_suvey a set lat_str = b.lat , lon_str = b.lon --(lat_str = b.lat , lon_str = b.lon // lat_end = b.lat , lon_end = b.lon
	from (
		select ST_LineInterpolatePoint(the_geom, persent) as the_geompoint,
		st_y(ST_LineInterpolatePoint(the_geom, persent)) as lat,
		st_x(ST_LineInterpolatePoint(the_geom, persent)) as lon, *
		from
		(
			select min(chainage_even) as ch_str, max(chainage_even) as ch_end, chainage,
			((chainage-min(chainage_even))::real / case when(max(chainage_even)-min(chainage_even))::real = 0 then 0.0001 else
			(max(chainage_even)-min(chainage_even))::real end)  as persent,file_name,
			ST_MakeLine(the_geom ORDER BY chainage) as the_geom
			from
			(
				select a.chainage, b.chainage as chainage_even, a.file_name,the_geom
				from
				(select chainage,file_name
				from
				( --เชค
					select even.chainage, min(b.chainage) as chainage_data, direction, event_name, link_id, even.the_geom,even.file_name
					from
					( --sum event
						--create table zz_event_test as
						select event_str as chainage, event_name, link_id, section_id, km_start, km_end, length, lane_no, date,
						lon_str,lat_str,
						st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326) as the_geom, file_name, 'str' as direction
						from data_suvey a
						--where link_id = '33600040201L2CC01   '
						group by event_str, event_name, link_id, section_id, km_start, km_end, length, lane_no, date,
						st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326), file_name,lon_str,lat_str
						union
						select event_end as chainage, event_name, link_id, section_id, km_start, km_end, length, lane_no, date,
						lon_end,lat_end,
						st_setsrid(st_makepoint(lon_end::real ,lat_end::real),4326) as the_geom, file_name, 'end' as direction
						from data_suvey a
						--where link_id = '33600040201L2CC01   '
						group by event_end, event_name, link_id, section_id, km_start, km_end, length, lane_no, date,
						st_setsrid(st_makepoint(lon_end::real ,lat_end::real),4326), file_name,lon_end,lat_end
						order by file_name,chainage
					) even
					left join
					(
						select chainage,file_name, st_setsrid(st_makepoint(lon::real ,lat::real),4326) as the_geom
						from access_key a,
						(select * from access_valuelaser left join access_pic b on  chainage = chainage_pic::int and file_name = name_pic) b
						where chainage between (event_str-5) and (event_end+5) and replace(event_type, ' ', '') = 'pavetype.'
						and replace(link_id, ' ', '') != '' and file_name = name_pic and file_name = name_key
						order by file_name,chainage
					) b on st_dwithin(even.the_geom, b.the_geom, 0.00004)
					where even.chainage != b.chainage --edit 220206
					--even.chainage - b.chainage > 6
					group by even.chainage, direction, event_name, link_id, even.the_geom,even.file_name
					order by even.chainage, link_id
				) foo
				order by file_name,chainage) a,
				(select chainage,file_name, st_setsrid(st_makepoint(lon::real ,lat::real),4326) as the_geom
				from access_valuelaser where lat != 0 and lon != 0) b
				where a.chainage between  b.chainage -5 and b.chainage+5 and a.file_name = b.file_name
			) foo
			--where chainage_even - chainage_even > 0
			group by chainage,file_name
		) foo
		where persent between 0 and 1
	) b
	where b.chainage = event_str and a.file_name = b.file_name
	--b.chainage = event_str // b.chainage = event_end'''
cur_step41 = conPG.cursor()
cur_step41.execute(step41)
# conPG.commit()


##STEP_4.2  update position str and end (even) : END  ####################################################################################################################################
step42 = '''
update data_suvey a set lat_end = b.lat , lon_end = b.lon --(lat_str = b.lat , lon_str = b.lon // lat_end = b.lat , lon_end = b.lon
	from (
		select ST_LineInterpolatePoint(the_geom, persent) as the_geompoint,
		st_y(ST_LineInterpolatePoint(the_geom, persent)) as lat,
		st_x(ST_LineInterpolatePoint(the_geom, persent)) as lon, *
		from
		(
			select min(chainage_even) as ch_str, max(chainage_even) as ch_end, chainage,
			((chainage-min(chainage_even))::real / case when(max(chainage_even)-min(chainage_even))::real = 0 then 0.0001 else
			(max(chainage_even)-min(chainage_even))::real end)  as persent,file_name,
			ST_MakeLine(the_geom ORDER BY chainage) as the_geom
			from
			(
				select a.chainage, b.chainage as chainage_even, a.file_name,the_geom
				from
				(select chainage,file_name
				from
				( --เชค
					select even.chainage, min(b.chainage) as chainage_data, direction, event_name, link_id, even.the_geom,even.file_name
					from
					( --sum event
						--create table zz_event_test as
						select event_str as chainage, event_name, link_id, section_id, km_start, km_end, length, lane_no, date,
						lon_str,lat_str,
						st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326) as the_geom, file_name, 'str' as direction
						from data_suvey a
						--where link_id = '33600040201L2CC01   '
						group by event_str, event_name, link_id, section_id, km_start, km_end, length, lane_no, date,
						st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326), file_name,lon_str,lat_str
						union
						select event_end as chainage, event_name, link_id, section_id, km_start, km_end, length, lane_no, date,
						lon_end,lat_end,
						st_setsrid(st_makepoint(lon_end::real ,lat_end::real),4326) as the_geom, file_name, 'end' as direction
						from data_suvey a
						--where link_id = '33600040201L2CC01   '
						group by event_end, event_name, link_id, section_id, km_start, km_end, length, lane_no, date,
						st_setsrid(st_makepoint(lon_end::real ,lat_end::real),4326), file_name,lon_end,lat_end
						order by file_name,chainage
					) even
					left join
					(
						select chainage,file_name, st_setsrid(st_makepoint(lon::real ,lat::real),4326) as the_geom
						from access_key a,
						(select * from access_valuelaser left join access_pic b on  chainage = chainage_pic::int and file_name = name_pic) b
						where chainage between (event_str-5) and (event_end+5) and replace(event_type, ' ', '') = 'pavetype.'
						and replace(link_id, ' ', '') != '' and file_name = name_pic and file_name = name_key
						order by file_name,chainage
					) b on st_dwithin(even.the_geom, b.the_geom, 0.00004)
					where even.chainage != b.chainage --edit 220206
					--even.chainage - b.chainage > 6
					group by even.chainage, direction, event_name, link_id, even.the_geom,even.file_name
					order by even.chainage, link_id
				) foo
				order by file_name,chainage) a,
				(select chainage,file_name, st_setsrid(st_makepoint(lon::real ,lat::real),4326) as the_geom
				from access_valuelaser where lat != 0 and lon != 0) b
				where a.chainage between  b.chainage -5 and b.chainage+5 and a.file_name = b.file_name
			) foo
			--where chainage_even - chainage_even > 0
			group by chainage,file_name
		) foo
		where persent between 0 and 1
	) b
	where b.chainage = event_end and a.file_name = b.file_name
	--b.chainage = event_str // b.chainage = event_end
'''
cur_step42 = conPG.cursor()
cur_step42.execute(step42)
conPG.commit()

##STEP_3.2  chcek_gps_loss ####################################################################################################################################

str_end = '''
select * from
	(select chainage,chainage_data,direction,event_name,link_id
			from
			(
				select even.chainage, min(b.chainage) as chainage_data, direction, event_name, link_id, even.the_geom
				from
				( --sum event
					select event_str as chainage, event_name, link_id,ramp_id, section_id, km_start, km_end, length, lane_no, date,
					st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326) as the_geom, file_name, 'str' as direction
					from data_suvey a
					--where link_id = '33600040201L2CC01   '
					group by event_str, event_name, link_id,ramp_id, section_id, km_start, km_end, length, lane_no, date,
					st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326), file_name
					union
					select event_end as chainage, event_name, link_id,ramp_id, section_id, km_start, km_end, length, lane_no, date,
					st_setsrid(st_makepoint(lon_end::real ,lat_end::real),4326) as the_geom, file_name, 'end' as direction
					from data_suvey a
					--where link_id = '33600040201L2CC01   '
					group by event_end, event_name, link_id,ramp_id, section_id, km_start, km_end, length, lane_no, date,
					st_setsrid(st_makepoint(lon_end::real ,lat_end::real),4326), file_name
				) even
				left join
				(
					select chainage, st_setsrid(st_makepoint(lon::real ,lat::real),4326) as the_geom
					from access_key a,
					(select * from access_valuelaser left join access_pic b on  chainage = chainage_pic::int) b
					where chainage between (event_str-5) and (event_end+5) and replace(event_type, ' ', '') = 'pavetype.'
					and replace(link_id, ' ', '') != ''
				) b on st_dwithin(even.the_geom, b.the_geom, 0.00004)
				where even.chainage - b.chainage > 6
				group by even.chainage, direction, event_name, link_id, even.the_geom
				order by even.chainage, link_id
			) foo)foo
where right(chainage::text,2) = '00' or 
	  right(chainage::text,2) = '25' or 
	  right(chainage::text,2) = '50' or 
	  right(chainage::text,2) = '75' '''
cur_step32 = conPG.cursor()
cur_step32.execute(str_end)
conPG.commit()
my_table1 = pd.read_sql(str_end, conPG)
pd.options.display.max_columns = None
pd.options.display.width=None
print('###############link_id ที่มีปัญหา km_str และ km_end :##################' )
print(my_table1)
print('______________________________________________________________________')

#
##STEP_3.3  chcek_gps_loss รอยต่อ  ####################################################################################################################################
gps_lo = '''
select gps_lost_ch,link_id,chainage_str,chainage_end
		from
		(
			select a.chainage as gps_lost_ch, c_p, meter, a.link_id, a.date,
			min(b.chainage) as chainage_str, max(b.chainage) as chainage_end,
			(max(b.chainage) -min(b.chainage))::real as persent,
			--(a.chainage- min(b.chainage))::real/ (max(b.chainage) -min(b.chainage))::real as persent,
			ST_MakeLine(the_geom ORDER BY b.chainage) as the_geom, file_name
			from gps_lost a, data_suvey b
			where b.chainage between new_p_min and new_p_max and lon != 0
			and st_y(the_geom) > 0  and status = 'มีGPSlost'
			group by gps_lost_ch, c_p, meter, a.link_id, a.date, file_name
		) foo
		where persent = 0
order by gps_lost_ch'''
cur_step33 = conPG.cursor()
cur_step33.execute(gps_lo)
conPG.commit()
my_table2 = pd.read_sql(gps_lo, conPG)
pd.options.display.max_columns = None
pd.options.display.width=None
print('###############Check Gps_lost ช่วงรอยต่อระหว่าง link_id :##################' )
print(my_table2)
print('______________________________________________________________________')

#
##STEP_3.4  chcek iri rut mpd and pic  ####################################################################################################################################
irmp = '''
select link_id,chainage,iri,rutting,texture,frame_number
		from data_suvey
		where iri is null or rutting is null or texture is null or frame_number is null
		and split_part((chainage/ CAST(25 AS float))::text, '.', 2) = ''  '''
cur_step34 = conPG.cursor()
cur_step34.execute(irmp)
conPG.commit()
my_table3 = pd.read_sql(irmp, conPG)
pd.options.display.max_columns = None
pd.options.display.width=None
print('###############chcek  ค่า : iri rut mpd and pic :##################' )
print(my_table3)
print('______________________________________________________________________')


##STEP_3.5  chcek link_id กรณีขา R และ L ขัดกับ km_start , km_end ####################################################################################################################################
c_link  = '''select *
		from
		(
			select row_number() over (order by km_start) as id, link_id, km_start, km_end, lane_no, lane_group,
			case 	when lane_group > 0 and km_end - km_start < 0 then 'Link_id ระบุฝั่ง L ผิด ตรวจสอบ Link_id และ lane_no'
				when lane_group < 0 and km_start - km_end < 0 then 'Link_id ระบุฝั่ง R ผิด ตรวจสอบ Link_id และ lane_no'
				when lane_no is null then 'Link_id ระบุผิดไม่ระบุทางหลัก ตรวจสอบ Link_id และ lane_no'
				when left(lane_no,1) = 'F' and abs(lane_group) != 2 then 'Link_id ระบุฝั่งประเภททางขนานผิด ตรวจสอบ Link_id และ lane_no'
				when left(lane_no,1) = 'I' and abs(lane_group) != 3 then 'Link_id ระบุฝั่งประเภททาง Interchange ผิด ตรวจสอบ Link_id และ lane_no'
				when left(lane_no,1) = 'U' and abs(lane_group) != 4 then 'Link_id ระบุฝั่งประเภททาง U-Trun ผิด ตรวจสอบ Link_id และ lane_no'
				when left(lane_no,1) = 'B' and abs(lane_group) != 5 then 'Link_id ระบุฝั่งประเภททางสะพานผิด ตรวจสอบ Link_id และ lane_no'
				when left(lane_no,1) = 'T' and abs(lane_group) != 6 then 'Link_id ระบุฝั่งประเภททางอุโมงค์ผิด ตรวจสอบ Link_id และ lane_no'
			end "ตรวจสอบ"
			from
			(
				SELECT date, event_name, link_id, section_id, lane_no,
				replace(km_start, '+', '')::int as km_start, replace(km_end, '+', '')::int as km_end,
				case 	when left(right(link_id,6),2) = 'L1' then 1
						when left(right(link_id,6),2) = 'L2' then 2
						when left(right(link_id,6),2) = 'L3' then 3
						when left(right(link_id,6),2) = 'L4' then 4
						when left(right(link_id,6),2) = 'L5' then 5
						when left(right(link_id,6),2) = 'L6' then 6
						when left(right(link_id,6),2) = 'R1' then -1
						when left(right(link_id,6),2) = 'R2' then -2
						when left(right(link_id,6),2) = 'R3' then -3
						when left(right(link_id,6),2) = 'R4' then -4
						when left(right(link_id,6),2) = 'R5' then -5
						when left(right(link_id,6),2) = 'R6' then -6
						else 0	end lane_group
				FROM data_suvey
				group by date, event_name,
				link_id, section_id, km_start, km_end, lane_no, lane_group
			) foo
		) foo
		where "ตรวจสอบ" is not null'''
cur_step35 = conPG.cursor()
cur_step35.execute(c_link)
conPG.commit()
my_table4 = pd.read_sql(c_link, conPG)
pd.options.display.max_columns = None
pd.options.display.width=None
print('###############chcek link_id กรณีขา R และ L ขัดกับ km_start , km_end##################' )
print(my_table4)
print('______________________________________________________________________')
print('')
print('STEP แก้ไข GPS LOST และ GPS ซ้ำ อย่าลืม ดู gps ในแผนที่ !!!!! successfully' )
##STEP_3.6  update ช่วงที่ต้องการค้นหา โดยกรอก ช่วง max min ที่พบค่า lat lon เป็น 0 ####################################################################################################################################
step5 = '''
update data_suvey a set lat = b.lat , lon = b.lon , the_geom = the_geompoint
	from (  --create table test_ssa2 as
		select ST_LineInterpolatePoint(the_geom, persent) as the_geompoint,
		st_y(ST_LineInterpolatePoint(the_geom, persent)) as lat,
		st_x(ST_LineInterpolatePoint(the_geom, persent)) as lon, *
		from
		(
			select *, p1/p2 as persent
			from
			(
				select a.chainage as gps_lost_ch, c_p, meter, a.link_id, a.date,
				min(b.chainage) as chainage_str, max(b.chainage) as chainage_end,
				(a.chainage- min(b.chainage))::real p1, (max(b.chainage) -min(b.chainage))::real as p2,
				--(a.chainage- min(b.chainage))::real/ (max(b.chainage) -min(b.chainage))::real as persent,
				ST_MakeLine(the_geom ORDER BY b.chainage) as the_geom, file_name
				from gps_lost a, data_suvey b
				where b.chainage between new_p_min and new_p_max and lon != 0
				and st_y(the_geom) > 0  and status = 'มีGPSlost'
				group by gps_lost_ch, c_p, meter, a.link_id, a.date, file_name
			) foo
			where p2 > 0
		) foo ) b
	where a.file_name = b.file_name and a.date = b.date and a.link_id = b.link_id and a.chainage = b.gps_lost_ch
'''
cur_step5 = conPG.cursor()
cur_step5.execute(step5)
conPG.commit()
print("STEP update GPS_LOST successfully ")

##STEP_3.7  update แก้ไข GPS ซ้ำ  ####################################################################################################################################
step66 = '''
update data_suvey a set lat = b.lat , lon = b.lon , the_geom = the_geompoint
from
(select chainage,link_id,ST_LineInterpolatePoint(the_geom, persent) as the_geompoint,
 		st_y(ST_LineInterpolatePoint(the_geom, persent)) as lat,
		st_x(ST_LineInterpolatePoint(the_geom, persent)) as lon
from
	(select *,p1/p2 as persent
	from
		(select b.chainage,b.link_id,b.grp1,a.min_c,a.max_c,
			(b.chainage - a.min_c)::real as p1,(a.max_c -a.min_c)::real as p2,a.the_geom
		from
			(select min_c,max_c,link_id,st_setsrid(ST_MakeLine(the_geom order by chainage),4326) AS the_geom
			from
				(select b.chainage,b.link_id,b.the_geom,grp1,min_c,max_c
				from
					(select  min(chainage)-25 as min_c,max(chainage)+25 as max_c,link_id,grp1
					from
						(select a.chainage,a.link_id,a.lat,a.lon,
								DENSE_RANK() over (order by a.the_geom) as grp1,
								a.the_geom
						from
							(select link_id,chainage,lat,lon,the_geom from
									(	select chainage, lat, lon, event_str, event_end, link_id, date,the_geom
										from data_suvey
										where split_part((chainage/ CAST(25 AS float))::text, '.', 2) = '' and (lon != 0 or lat != 0)
										and status = 'มีGPSlost')foo
							order by link_id,chainage,lat,lon,the_geom ) a
						left join
							(select min(chainage),max(chainage),link_id,the_geom,count(link_id) from data_suvey
							 where split_part((chainage/ CAST(25 AS float))::text, '.', 2) = '' and (lon != 0 or lat != 0)
							 group by link_id,the_geom
							 HAVING COUNT(the_geom) > 1
							) b
						on a.the_geom = b.the_geom
						where a.the_geom = b.the_geom) foo
				group by link_id,grp1) a
				left join data_suvey b on a.min_c = b.chainage or a.max_c = b.chainage)foo
			group by link_id,grp1,min_c,max_c) a
			,
			(select chainage,link_id,grp1
			from
				(select a.chainage,a.link_id,a.lat,a.lon,
						DENSE_RANK() over (order by a.the_geom) as grp1,
						a.the_geom
				from
					(select link_id,chainage,lat,lon,the_geom from
							(	select chainage, lat, lon, event_str, event_end, link_id, date,the_geom
								from data_suvey
								where split_part((chainage/ CAST(25 AS float))::text, '.', 2) = '' and (lon != 0 or lat != 0)
								and status = 'มีGPSlost')foo
					order by link_id,chainage,lat,lon,the_geom ) a
				left join
					(select min(chainage),max(chainage),link_id,the_geom,count(link_id) from data_suvey
					 where split_part((chainage/ CAST(25 AS float))::text, '.', 2) = '' and (lon != 0 or lat != 0)
					 group by link_id,the_geom
					 HAVING COUNT(the_geom) > 1) b
				on a.the_geom = b.the_geom
				where a.the_geom = b.the_geom) foo
			group by grp1,chainage,link_id) b
		where (b.chainage - a.min_c)::real > 0 and (b.chainage - a.min_c)::real < (a.max_c -a.min_c)::real
		group by b.chainage,b.link_id,b.grp1,a.min_c,a.max_c,a.the_geom)foo)foo) b
where a.chainage = b.chainage'''
cur_step66 = conPG.cursor()
cur_step66.execute(step66)
conPG.commit()
print("STEP update GPS_ซ้ำ successfully ")

##STEP_3.8  update ค่าสภาพทางเฉลี่ย ####################################################################################################################################
step55 = '''
update data_suvey a set iri_right=b.iri_right,iri_left=b.iri_left,iri=b.iri,iri_lane=b.iri_lane,
rutt_right=b.rutt_right,rutt_left=b.rutt_left,rutting=b.rutting,texture=b.texture,etd_texture=b.etd_texture
from
	(	select chainage,
		((iri_right_o+iri_right_d)/9)::numeric(8,2) as iri_right,
		((iri_left_o+iri_left_d)/9)::numeric(8,2) as iri_left,
		(((((iri_left_o+iri_left_d)/9))+(((iri_right_o+iri_right_d)/9)))/2)::numeric(8,2) as iri,
		((iri_lane_o+iri_lane_d)/9)::numeric(8,2) as iri_lane,
		((rutt_right_o+rutt_right_d)/9)::numeric(8,2) as rutt_right,
		((rutt_left_o+rutt_left_d)/9)::numeric(8,2) as rutt_left,
		(((((rutt_left_o+rutt_left_d)/9))+(((rutt_right_o+rutt_right_d)/9)))/2)::numeric(8,2) as rutting,
		((texture_o+texture_d)/9)::numeric(8,2) as texture,
		((etd_texture_o+etd_texture_d)/9)::numeric(8,2) as etd_texture
		from
			(select chainage,
			 iri_right_o::numeric(8,2),((iri_right_d::numeric(8,2))-iri_right) as iri_right_d,
			 iri_left_o::numeric(8,2),((iri_left_d::numeric(8,2))-iri_left) as iri_left_d,
			 iri_lane_o::numeric(8,2),((iri_lane_d::numeric(8,2))-iri_lane) as iri_lane_d,
			 rutt_right_o::numeric(8,2),((rutt_right_d::numeric(8,2))-rutt_right) as rutt_right_d,
			 rutt_left_o::numeric(8,2),((rutt_left_d::numeric(8,2))-rutt_left) as rutt_left_d,
			 texture_o::numeric(8,2),((texture_d::numeric(8,2))-texture) as texture_d,
			 etd_texture_o::numeric(8,2),((etd_texture_d::numeric(8,2))-etd_texture) as etd_texture_d
			from
				(SELECT
					chainage,iri_right::numeric(8,2),iri_left::numeric(8,2),iri_lane::numeric(8,2),rutt_right::numeric(8,2),rutt_left::numeric(8,2),
					texture::numeric(8,2),etd_texture::numeric(8,2),
					sum(iri_right::numeric(8,2)) over (order by chainage ROWS 4 PRECEDING ) as iri_right_o,
					sum(iri_right::numeric(8,2)) over (order by chainage ROWS BETWEEN CURRENT ROW and 4 FOLLOWING) as iri_right_d,
					sum(iri_left::numeric(8,2)) over (order by chainage ROWS 4 PRECEDING ) as iri_left_o,
					sum(iri_left::numeric(8,2)) over (order by chainage ROWS BETWEEN CURRENT ROW and 4 FOLLOWING) as iri_left_d,
					sum(iri_lane::numeric(8,2)) over (order by chainage ROWS 4 PRECEDING ) as iri_lane_o,
					sum(iri_lane::numeric(8,2)) over (order by chainage ROWS BETWEEN CURRENT ROW and 4 FOLLOWING) as iri_lane_d,
					sum(rutt_right::numeric(8,2)) over (order by chainage ROWS 4 PRECEDING ) as rutt_right_o,
					sum(rutt_right::numeric(8,2)) over (order by chainage ROWS BETWEEN CURRENT ROW and 4 FOLLOWING) as rutt_right_d,
					sum(rutt_left::numeric(8,2)) over (order by chainage ROWS 4 PRECEDING ) as rutt_left_o,
					sum(rutt_left::numeric(8,2)) over (order by chainage ROWS BETWEEN CURRENT ROW and 4 FOLLOWING) as rutt_left_d,
					sum(texture::numeric(8,2)) over (order by chainage ROWS 4 PRECEDING ) as texture_o,
					sum(texture::numeric(8,2)) over (order by chainage ROWS BETWEEN CURRENT ROW and 4 FOLLOWING) as texture_d,
					sum(etd_texture::numeric(8,2)) over (order by chainage ROWS 4 PRECEDING ) as etd_texture_o,
					sum(etd_texture::numeric(8,2)) over (order by chainage ROWS BETWEEN CURRENT ROW and 4 FOLLOWING) as etd_texture_d
					FROM data_suvey
					order by chainage
				)foo
			)foo
	) b
where a.chainage = b.chainage
'''
cur_step55 = conPG.cursor()
cur_step55.execute(step55)
conPG.commit()


##STEP_4  create survey  ####################################################################################################################################
#s_id = str(input('select max(survey_id) from survey ='))
step7 = '''
create table survey as --delete from survey_local
select
	row_number() over (order by chainage_str::int,link_id)+1 as survey_id, null::int as subsection_id, section_id, link_id,ramp_id,
	left(right(link_id,4),2)||left(link_id,11) as survey_code, 	---'CC41603470101'
	(right(link_id,2)::int)::character(25) as run_code, 
	case 	when left(right(link_id,6),2) = 'L1' then 1
		when left(right(link_id,6),2) = 'L2' then 2
		when left(right(link_id,6),2) = 'L3' then 3
		when left(right(link_id,6),2) = 'L4' then 4
		when left(right(link_id,6),2) = 'L5' then 5
		when left(right(link_id,6),2) = 'L6' then 6
		when left(right(link_id,6),2) = 'L7' then 7
		when left(right(link_id,6),2) = 'R1' then -1
		when left(right(link_id,6),2) = 'R2' then -2
		when left(right(link_id,6),2) = 'R3' then -3
		when left(right(link_id,6),2) = 'R4' then -4
		when left(right(link_id,6),2) = 'R5' then -5
		when left(right(link_id,6),2) = 'R6' then -6
		when left(right(link_id,6),2) = 'R7' then -7
	else 0	end lane_group, 	---lane_group ==> 1=L , -1=R, 2=FL, -2=FR, 3=IL, -3 = IR, 4 = UL, -4 = UR, 5=BL,  -5 = BR, 6 = TL, -6 = TR
	right(lane_no,1)::int as lane_no, null::int as lane_reverse,file_name, km_start, km_end, 
	case when left(left(right(link_id,6),2),1) = 'L' then (km_end-km_start)::int
		when left(left(right(link_id,6),2),1) = 'R' then (km_start-km_end)::int end length, 
	(st_length(the_geom::geography)/1000)::numeric(8,3) as distance_odo, (st_length(the_geom::geography)/1000)::numeric(8,3) as distance_gps, left(date::text,4)::int as year,
	case	when left(right(link_id,4),2) = 'AC' then 2
		when left(right(link_id,4),2) = 'CC' then 1
	 end survey_type, 						---1= CC , 2 =AC
	date, the_geom, 'CU_survey'::character(10) as remark, 'BTS22'::character(10) as run_new, 25::int as interval
	from
	(
		select min(chainage) as chainage_str, max(chainage) as chainage_end, event_name, link_id,ramp_id, section_id::int, 
		replace(km_start, '+', '')::int as km_start, replace(km_end, '+', '')::int as km_end, (length::real)*1000 as length, 
		case when right(lane_no,1) = 'L' then lane_no||'2'
		     when right(lane_no,1) = 'R' then lane_no||'2' else lane_no end lane_no, date,
		st_setsrid(ST_MakeLine(the_geom ORDER BY file_name,chainage),4326) AS the_geom, file_name
		from
		(	
			--เชคจุดเส้น create table test_point_survey as 
			select *
			from 
			( --sum event
				select event_str as chainage, event_name, link_id,ramp_id, section_id, km_start, km_end, length, lane_no, date,
				st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326) as the_geom, file_name
				from data_suvey a
				group by event_str, event_name, link_id,ramp_id, section_id, km_start, km_end, length, lane_no, date, 
				st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326), file_name
				union
				select event_end as chainage, event_name, link_id,ramp_id, section_id, km_start, km_end, length, lane_no, date,
				st_setsrid(st_makepoint(lon_end::real ,lat_end::real),4326) as the_geom, file_name
				from data_suvey a
				group by event_end, event_name, link_id,ramp_id, section_id, km_start, km_end, length, lane_no, date, 
				st_setsrid(st_makepoint(lon_end::real ,lat_end::real),4326), file_name
				order by file_name,chainage
			) even
			union
				select chainage, event_name, link_id,ramp_id, section_id, km_start, km_end, length, lane_no, date, the_geom, file_name
				from data_suvey
				where chainage between event_str and event_end and (st_x(the_geom) > 0 or st_y(the_geom) > 0)
				and split_part((chainage/ CAST(25 AS float))::text, '.', 2) = ''
				order by file_name,chainage
		) foo
		where link_id != 'construction' and replace(length, ' ', '')::real > 0
		group by event_name, link_id, section_id, km_start, km_end, length, lane_no, date, file_name,ramp_id
	) foo'''

cur_step7 = conPG.cursor()
cur_step7.execute(step7)
conPG.commit()

##STEP_5  create survey_point_local  ####################################################################################################################################
#s_point_id = str(input('select max(survey_point_id) from survey_point ='))
step8 = '''
create table survey_point_local as
	select  row_number() over (order by km::int)+1 as survey_point_id, *
	from
	(
		select row_number() over (partition by a.date, a.link_id order by  a.chainage)-1 as order_row,
		case
		when lane_group > 0 then ((row_number() over (partition by a.date, survey_id, a.link_id order by  a.chainage)-1)*25)+km_start
		when lane_group < 0 then km_start-((row_number() over (partition by a.date, survey_id, a.link_id order by  a.chainage)-1)*25)
		end km,
		a.*, survey_id, survey_code, run_code
		from
		(
			select *
			from
			(
				select even.chainage, even.link_id, even.date, even.the_geom, even.file_name,ramp_id,
				avg(iri_right::real)::numeric(8,2) as iri_right,
				avg(iri_left::real)::numeric(8,2) as iri_left,
				avg(iri::real)::numeric(8,2) as iri,
				avg(iri_lane::real)::numeric(8,2) as iri_lane,
				avg(rutt_right::real)::numeric(8,2) as rutt_right,
				avg(rutt_left::real)::numeric(8,2) as rutt_left,
				avg(rutting::real)::numeric(8,2) as rutting,
				avg(texture::real)::numeric(8,2) as texture,
				avg(etd_texture::real)::numeric(8,2) as etd_texture
				from
				(
					select event_str as chainage, link_id, date,ramp_id,
					st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326) as the_geom, file_name
					from data_suvey a
					where event_str::int != chainage::int
					group by event_str, link_id, date, ramp_id,
					st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326), file_name
					/*--edit 220119 /date
					union
					select event_end as chainage, link_id, date,
					st_setsrid(st_makepoint(lon_end::real ,lat_end::real),4326) as the_geom, file_name
					from data_suvey a
					group by event_end, link_id, date,
					st_setsrid(st_makepoint(lon_end::real ,lat_end::real),4326), file_name
					*/
				)  even
				left join
				(
					select 	chainage ,case when event_str - chainage >= 0 then event_str
								when chainage - event_str < 5 then event_str
								when event_end - chainage >= 0 then event_end
								when chainage - event_end < 5 then event_end
							end chainages, link_id, date, the_geom, file_name,
						iri_right, iri_left, iri, iri_lane, rutt_right, rutt_left, rutting, texture, etd_texture
					from data_suvey
					where event_str between chainage-6 and chainage+6 or event_end between chainage-4 and chainage+4
				) b on  even.chainage = b.chainages and even.file_name = b.file_name and even.link_id = b.link_id and even.date = b.date
				where even.chainage::int != b.chainage::int
				group by even.chainage, even.link_id, even.date, even.the_geom, even.file_name,ramp_id
			) a

			union

				select b.chainage, a.link_id, a.date, the_geom, a.file_name, ramp_id,a.iri_right, a.iri_left, a.iri,
				a.iri_lane, a.rutt_right, a.rutt_left, a.rutting, a.texture, a.etd_texture
				from
				(select * from
						(select  right((cast(chainage as varchar(10))),2) as chainage_s,a.chainage ,link_id, date, file_name, --edit 220206 /date
							avg(iri_right::real)::numeric(8,2) as iri_right,
							avg(iri_left::real)::numeric(8,2) as iri_left,
							avg(iri::real)::numeric(8,2) as iri,
							avg(iri_lane::real)::numeric(8,2) as iri_lane,
							avg(rutt_right::real)::numeric(8,2) as rutt_right,
							avg(rutt_left::real)::numeric(8,2) as rutt_left,
							avg(rutting::real)::numeric(8,2) as rutting,
							avg(texture::real)::numeric(8,2) as texture,
							avg(etd_texture::real)::numeric(8,2) as etd_texture
						from
							(
							select a.*, iri_right, iri_left, iri, iri_lane, rutt_right,
								rutt_left, rutting, texture, etd_texture,b.ramp_id
							from
							(
								select chainage, event_str, event_end, link_id, date, file_name,ramp_id
								from data_suvey
								where chainage between event_str and event_end
							) a
							left join
							(
								select chainage, iri_right, iri_left, iri, iri_lane, rutt_right,
								rutt_left, rutting, texture, etd_texture,ramp_id
								from data_suvey
								where chainage between event_str and event_end
								union
								select *
								from
								(
									select b.chainage, b.iri_right, b.iri_left, b.iri, b.iri_lane, b.rutt_right,
									b.rutt_left, b.rutting, b.texture, b.etd_texture,ramp_id
									from data_suvey a
									left join access_valuelaser b on a.chainage = b.chainage-5
									where b.chainage > event_end
									order by a.chainage
									--limit 1
								) foo
								order by chainage

							) b on a.chainage = b.chainage-5
							order by a.chainage
						) a
					where chainage between event_str and event_end --or chainage_s = '00'   --edit 220206 /date
					group by chainage_s ,a.chainage, link_id, date, file_name
					order by chainage) foo
					where chainage_s = '00' or chainage_s = '25' or
						  chainage_s= '50' or chainage_s = '75') a
					left join data_suvey b on a.chainage = b.chainage and a.file_name = b.file_name
					where the_geom is not null
			--order by b.chainage
		) a
		left join survey b on a.link_id = b.link_id
	) foo
	where link_id != 'construction'
	order by chainage'''
cur_step8 = conPG.cursor()
cur_step8.execute(step8)
conPG.commit()

##STEP_9  create survey_point  ####################################################################################################################################
step9 = '''--create survey_point to php
create table survey_point as
select --chainage, order_row, survey_point_id, survey_id,  km, iri, st_x(the_geom) as lon, st_y(the_geom) as lat, the_geom-- test
survey_point_id, survey_id, km, iri_right, iri_left, iri, iri_lane,
	rutt_right, rutt_left, rutting, texture, etd_texture, the_geom, left(link_id,3)||'_'||left(date::text,4) as remark
from survey_point_local
order by  survey_id, chainage, survey_point_id'''
cur_step9 = conPG.cursor()
cur_step9.execute(step9)
conPG.commit()

##STEP_10  create survey_image  ####################################################################################################################################
step10 = '''create table survey_image as
select 's22/cu_survey/'||left(a.link_id,3)||'/'||date||'/'||survey_code||'/Run'||run_code||'/image/'||filename as directory,
filename, date, a.chainage as img_id,km, 'True'::text as imagepath, the_geom, survey_id, 'CU_S22'::character(10) as remark
from
(
	select survey_point_id, survey_id, km, a.link_id, a.chainage, a.file_name, frame_number, a.date, survey_code, run_code,
	a.file_name||'-ROW-0-'||case when length(frame_number::text) = 1 then '0000'||frame_number
				when length(frame_number::text) = 2 then '000'||frame_number::text
				when length(frame_number::text) = 3 then '00'||frame_number::text
				when length(frame_number::text) = 4 then '0'||frame_number::text
				when length(frame_number::text) > 4 then frame_number::text end||'.jpg' as filename,
	a.the_geom
	from survey_point_local a
	left join data_suvey b on case when (right(((a.chainage::int)::text), 1))::int = 6 then (a.chainage::int)+4
				       when (right(((a.chainage::int)::text), 1))::int = 7 then (a.chainage::int)+3
				       when (right(((a.chainage::int)::text), 1))::int = 8 then (a.chainage::int)+2
				       when (right(((a.chainage::int)::text), 1))::int = 9 then (a.chainage::int)+1
				       when (right(((a.chainage::int)::text), 1))::int = 0 then (a.chainage::int)+0
				       when (right(((a.chainage::int)::text), 1))::int = 4 then (a.chainage::int)+1
				       when (right(((a.chainage::int)::text), 1))::int = 3 then (a.chainage::int)+2
				       when (right(((a.chainage::int)::text), 1))::int = 2 then (a.chainage::int)+3
				       when (right(((a.chainage::int)::text), 1))::int = 1 then (a.chainage::int)+4
				       when (right(((a.chainage::int)::text), 1))::int = 5 then (a.chainage::int)+0
				        else a.chainage::int end  = chainage_pic::int
	--where survey_id = 1263740085
	group by survey_point_id, survey_id, km, a.link_id, a.chainage, a.file_name, frame_number, a.date, survey_code, run_code,
	filename, a.the_geom
	order by filename --survey_point_id
) a
order by img_id'''
cur_step10 = conPG.cursor()
cur_step10.execute(step10)
conPG.commit()
print('step : create survey,survey_point,survey_image successfully')
print('------------------------------------------------------------------------------------------------------------')
print('___DATA-PAVE-PROCESSING____________________________________________________________________')
##STEP_11  create data_pave_suvey  ####################################################################################################################################

step_11 = '''create table data_pave_suvey as
	select b.survey_id,a.chainage_pic::int,a.event_str,a.event_end,a.link_id,a.frame_number,b.lane_group,b.lane_no,a.lon,a.lat,b.date,a.the_geom,a.lat_str,a.lat_end,a.lon_str,a.lon_end
	,b.km_start,b.km_end,a.file_name,b.run_code,b.survey_code
	from
		(select a.event_str,a.event_end,a.link_id,b.chainage_pic,b.frame_number,b.lon,b.lat,a.lat_str,a.lat_end,a.lon_str,a.lon_end,
		 st_setsrid(st_makepoint(b.lon::real ,b.lat::real),4326) as the_geom,a.survey_date,a.file_name
		from
			(select event_str,event_end,event_name,link_id,survey_date::date,lat_str,lat_end,lon_str,lon_end,file_name from data_suvey
			group by event_str,event_end,event_name,link_id,survey_date,lat_str,lat_end,lon_str,lon_end,file_name
			order by event_str) a
		left join  access_distress_pic b on b.chainage_pic between a.event_str and a.event_end and a.file_name = b.name_pave
		order by a.file_name,b.chainage_pic) a
	left join survey b on a.link_id = b.link_id and a.survey_date = b.date
	order by a.file_name,a.chainage_pic'''
cur_step11 = conPG.cursor()
cur_step11.execute(step_11)
conPG.commit()
print('step : create data_pave_suvey successfully')

step_11_1 = '''create table gps_lost_pave as
		select a.*, b.chainage_pic, min-3 as new_p_min, max+3 as new_p_max
		from
		(
			--max min ช่วง GPS ที่เริ่มหาย
			select min(chainage_pic) as min, max(chainage_pic) as max, count(*) as c_p, count(*)*3 as meter,
			link_id, date, grp2
			from
			(
				select chainage_pic, lat, lon, survey_id, link_id,date,
				row_number() OVER (partition by lat, lon, link_id order by  chainage_pic) as grp1,
				row_number() over (partition by date, link_id order by  chainage_pic)  -   --(minus)
				row_number() OVER (partition by lat, lon, link_id order by  chainage_pic) as grp2
				from data_pave_suvey
				order by chainage_pic
			) foo
			where (lon = 0 or lat = 0)
			group by grp2, link_id, date, grp2
			order by min
		) a,
		(
		select chainage_pic, lat, lon, event_str, event_end, link_id, date
		from data_pave_suvey
		where  (lon = 0 or lat = 0)
		) b
		where chainage_pic between min and max
		group by min, max, c_p, meter, a.link_id, a.date, grp2, b.chainage_pic, new_p_min, new_p_max
		order by min'''
cur_step11_1 = conPG.cursor()
cur_step11_1.execute(step_11_1)
conPG.commit()

step_11_2 = '''update data_pave_suvey a set lat = b.lat , lon = b.lon , the_geom = the_geompoint
	from (  --create table test_ssa2 as
		select ST_LineInterpolatePoint(the_geom, persent) as the_geompoint,
		st_y(ST_LineInterpolatePoint(the_geom, persent)) as lat,
		st_x(ST_LineInterpolatePoint(the_geom, persent)) as lon, *
		from
		(
			select *, p1/p2 as persent
			from
			(
				select a.chainage_pic as gps_lost_ch, c_p, meter, a.link_id, a.date,b.survey_id,
				min(b.chainage_pic) as chainage_str, max(b.chainage_pic) as chainage_end,
				(a.chainage_pic- min(b.chainage_pic))::real p1, (max(b.chainage_pic) -min(b.chainage_pic))::real as p2,
				--(a.chainage- min(b.chainage))::real/ (max(b.chainage) -min(b.chainage))::real as persent,
				c.the_geom
				from gps_lost_pave a, data_pave_suvey b,survey c
				where b.chainage_pic between new_p_min and new_p_max and b.survey_id = c.survey_id
				group by gps_lost_ch, c_p, meter, a.link_id, a.date,c.the_geom,b.survey_id
			) foo
			where p2 > 0
		) foo ) b
	where  a.date = b.date and a.link_id = b.link_id and a.chainage_pic = b.gps_lost_ch '''
cur_step11_2 = conPG.cursor()
cur_step11_2.execute(step_11_2)
conPG.commit()
print('step : แก้ไข GPS_LOST data_pave_suvey successfully')

step_11_3 = '''
update data_pave_suvey a set lat = b.lat , lon = b.lon , the_geom = the_geompoint
from
	(select  chainage_pic,link_id,ST_LineInterpolatePoint(the_geom, persent) as the_geompoint,
			st_y(ST_LineInterpolatePoint(the_geom, persent)) as lat,
			st_x(ST_LineInterpolatePoint(the_geom, persent)) as lon
	from
		(select *,p1/p2 as persent
		from
			(select survey_id,chainage_pic,link_id,grp1,the_geom,event_str,event_end,
			(chainage_pic-event_str)::real as p1,(event_end-event_str) as p2
			from
				(select b.survey_id,chainage_pic,a.link_id,a.grp1,b.the_geom,event_str,event_end
				from
					(select  chainage_pic,link_id,grp1,lat,lon,the_geom,event_str,event_end
					from
						(select a.chainage_pic,a.link_id,a.lat,a.lon,a.event_str,a.event_end,
								DENSE_RANK() over (order by a.the_geom) as grp1,
								a.the_geom
						from
							(select link_id,chainage_pic,lat,lon,the_geom,event_str, event_end
							 from
								(select chainage_pic, lat, lon, event_str, event_end, link_id, date,the_geom
								from data_pave_suvey
								where (lon != 0 or lat != 0))foo
							order by link_id,chainage_pic,lat,lon,the_geom ) a
						left join
							(select min(chainage_pic),max(chainage_pic),link_id,the_geom,count(link_id)
							 from data_pave_suvey
							 where  (lon != 0 or lat != 0)
							 group by link_id,the_geom
							 HAVING COUNT(the_geom) > 1) b
						on a.the_geom = b.the_geom
						where a.the_geom = b.the_geom) foo
					) a
				left join survey b   on a.link_id = b.link_id ) foo )foo
		 where p1/p2 > 0 and p1/p2 < 1
		 group by survey_id,chainage_pic,link_id,grp1,the_geom,event_str,event_end,p1,p2)foo) b
where  a.chainage_pic = b.chainage_pic
'''
cur_step11_3 = conPG.cursor()
cur_step11_3.execute(step_11_3)
conPG.commit()
print('step : แก้ไข GPS_ซ้ำ data_pave_suvey successfully')

step_11_4 = '''
create table survey_pave as
select ('cu_survey/'||left(link_id,3)||'/'||date||'/'||survey_code||'/Run'||run_code||'/pavement'
	||replace(split_part(filename, ' ', 2),' ', '')||'/'||filename)::character varying(254) as directory,
	filename::character varying(254) as filename,image_place,date,img_id,km::double precision as km,
	imagepath,the_geom,survey_id,'cu_survey'::character(10) as remark
from
	(select survey_id,link_id,file_name||'-PAVE-0-'||case when length(frame_number::text) = 1 then '0000'||(frame_number::int+1)::text
				when length(frame_number::text) = 2 then '000'||(frame_number::int+1)::text
				when length(frame_number::text) = 3 then '00'||(frame_number::int+1)::text
				when length(frame_number::text) = 4 then '0'||(frame_number::int+1)::text
				when length(frame_number::text) > 4 then (frame_number::int+1)::text end||'.jpg' as filename,
		chainage_pic,event_str,event_end,km_start,km_end,lane_group,lane_no,
				case when lane_group < 0 then km_start-(chainage_pic - event_str::int)
					when lane_group > 0 then km_start+(chainage_pic - event_str::int)
					end km,frame_number,frame_number::double precision as img_id,run_code,survey_code,
					TRUE::character varying(254) as imagepath,the_geom ,date,0::int as image_place
	from data_pave_suvey
order by chainage_pic) foo
'''
cur_step11_4 = conPG.cursor()
cur_step11_4.execute(step_11_4)
conPG.commit()
print('create survey_pave successfully')

##STEP_13  chack รูปซ้ำ  ###################################################################################################################################
imagee = '''select filename,cc
from
	(SELECT filename,count(filename) as cc
	from survey_image
	group by filename
	order by filename ) foo
where cc > 1 or filename is null'''
cur_step113 = conPG.cursor()
cur_step113.execute(imagee)
conPG.commit()
my_table6 = pd.read_sql(imagee, conPG)
pd.options.display.max_columns = None
pd.options.display.width=None

##STEP_133  แก้ไขรูปซ้ำ  ###################################################################################################################################
step133 = '''
update 	survey_image a set directory = b.directory, filename=b.filename
from
	(select  case when dir = 0 then split_part(directory,'image/',1)||'image/'|| split_part(filename,'-0-',1)||'-0-'||substring(split_part(filename,'-0-',2),1,4) ||'3'||'.jpg'
			 when dir = 5 then split_part(directory,'image/',1)||'image/'|| split_part(filename,'-0-',1)||'-0-'||substring(split_part(filename,'-0-',2),1,4) ||'7'||'.jpg'
			end directory,
		case when file_n = 0 then split_part(filename,'-0-',1)||'-0-'||substring(split_part(filename,'-0-',2),1,4) ||'3'||'.jpg'
			 when file_n = 5 then split_part(filename,'-0-',1)||'-0-'||substring(split_part(filename,'-0-',2),1,4) ||'7'||'.jpg'
			end filename, img_id,img_bf
	from
		(select directory,filename,substring(reverse(directory),5,1)::int as dir ,substring(reverse(filename),5,1)::int as file_n,img_id,cc,img_bf
		from
		(	SELECT a.directory,a.filename,a.img_id,b.cc,lag(img_id) over (order by img_id) as img_bf
				from survey_image a
			left join
				(select filename,cc
				from
					(SELECT filename,count(filename) as cc
					from survey_image
					group by filename
					order by filename ) foo
				where cc > 1 or filename is null) b
			on a.filename = b.filename
			where b.cc is not null) foo
		where img_id-img_bf < 25) foo) b
where a.img_id = b.img_id
'''
cur_step133 = conPG.cursor()
cur_step133.execute(step133)
conPG.commit()
print('')
print('------------------------------------------------------------------------------------------------------------')
print('รายงานการตรวจเสร็จสิ้น อย่าลืมตรวจสอบใน QGIS !!!!!')
print('------------------------------------------------------------------------------------------------------------')
##STEP_11  Dump SQL  ####################################################################################################################################
# dump1 = '''COPY (
# SELECT dump('public', 'survey','true')
# ) TO '%s\\%s\\survey.sql'; ''' % (path,a)
# cur_dump1 = conPG.cursor()
# cur_dump1.execute(dump1)
# conPG.commit()
#
# dump2 = '''COPY (
# SELECT dump('public', 'survey_point','true')
# ) TO '%s\\%s\\survey_point.sql'; ''' % (path,a)
# cur_dump2 = conPG.cursor()
# cur_dump2.execute(dump2)
# conPG.commit()
#
# dump3 = '''COPY (
# SELECT dump('public', 'survey_image','true')
# ) TO '%s\\%s\\survey_image.sql'; ''' % (path,a)
# cur_dump3 = conPG.cursor()
# cur_dump3.execute(dump3)
# conPG.commit()
#
# dump33 = '''COPY (
# SELECT dump('public', 'survey_pave','true')
# ) TO '%s\\%s\\survey_image.sql'; ''' % (path,a)
# cur_dump33 = conPG.cursor()
# cur_dump33.execute(dump33)
# conPG.commit()
# print('step : Dump SQL successfully')
#
# ##STEP_12  .bat file  ####################################################################################################################################
#
# dump4 = r'''COPY (
# SELECT 'mkdir' as test,'E:\'||'s22\row\image\'||SPLIT_PART((replace(directory,'/','\')),'\image',1)||'\image' as folder
# FROM survey_image
# group by test,folder
# ) TO '%s\%s\row_01mkdir.bat'; ''' % (path,a)
# cur_dump4 = conPG.cursor()
# cur_dump4.execute(dump4)
# conPG.commit()
#
# dump5 = r'''COPY (
# SELECT 'copy' as test,'E:\'||left(filename,8)||'\'||left(filename,8)||'\ROW-0\'||filename,
# 'E:\'||'s22\row\image\'||SPLIT_PART((replace(directory,'/','\')),'\image',1)||'\image'||filename
# FROM survey_image
# order by filename
# ) TO '%s\%s\row_02copy.bat'; ''' % (path,a)
# cur_dump5 = conPG.cursor()
# cur_dump5.execute(dump5)
# conPG.commit()
# print('step : Dump .bat file successfully')
# print('')
#
# dump6 = r'''COPY (
# SELECT 'mkdir' as test,'E:\'||'s22\pave\image\'||SPLIT_PART((replace(directory,'/','\')),'\image',1)||'\image' as folder
# FROM survey_pave
# group by test,folder
# ) TO '%s\%s\pave_01mkdir.bat'; ''' % (path,a)
# cur_dump6 = conPG.cursor()
# cur_dump6.execute(dump6)
# conPG.commit()
#
# dump7 = r'''COPY (
# SELECT 'copy' as test,'E:\'||left(filename,8)||'\'||left(filename,8)||'\ROW-0\'||filename,
# 'E:\'||'s22\pave\image\'||SPLIT_PART((replace(directory,'/','\')),'\image',1)||'\image'||filename
# FROM survey_pave
# order by filename
# ) TO '%s\%s\pave_02copy.bat'; ''' % (path,a)
# cur_dump7 = conPG.cursor()
# cur_dump7.execute(dump7)
# conPG.commit()
#