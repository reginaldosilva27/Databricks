
-- Table to log events
drop table tb_OrchestratorEvents;
CREATE TABLE tb_OrchestratorEvents
(
    id int IDENTITY PRIMARY KEY,
    jobName VARCHAR(200),
    jobId VARCHAR(200),
    databricksWorkspace VARCHAR(200),
    emailList VARCHAR(MAX),
    subject VARCHAR(MAX),
    customBody VARCHAR(MAX),
    dateLog datetime
)

-- Generate Event
INSERT INTO tb_OrchestratorEvents VALUES (
    'Job1-Teste',
    '981175440018532',
    'https://adb-4013955633331914.14.azuredatabricks.net/api/2.1/jobs/run-now',
    'reginaldo.silva@dataside.com.br',
    'LogicApp - Item criado na tabela tb_OrchestratorEvents',
    'Event Information: Job Run created ',
    GETDATE()
    )

--Events
select * from tb_OrchestratorEvents
