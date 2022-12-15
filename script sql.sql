#create database afto;
use afto;

create table llamadas(
fecha date,
rango_hora nchar(5),
hora_valida nchar(5),
campania varchar(5),
direccion varchar(20),
agente nvarchar(50),
estado_llamada nvarchar(30),
motivo nvarchar(100),
submotivo nvarchar(100),
tipo_emisor nvarchar(50),
tipo_cliente  nvarchar(50),
operador  nvarchar(50),
nivel_servicio  nvarchar(50),
NS int,
Wra_Aten int,
Aten_TMO int,
Ent int,
Aten int,
Aban int,
TMO int,
TME int
)

select count(1) from llamadas;

select * from llamadas limit 100;