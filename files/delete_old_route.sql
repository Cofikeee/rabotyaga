
-- Удаление содержимого маршрута перед обновлением;
SET SEARCH_PATH to ekd_ekd;

DO
$$
    DECLARE
        templateId uuid;
        stageId record;
    BEGIN
        templateId := '$templateId';

        FOR stageId IN (SELECT id
                        FROM signing_route_template_stage
                        WHERE template_id = templateId
                        ORDER BY index_number DESC)
            LOOP
                DELETE FROM signing_route_template_participant WHERE template_stage_id = stageId.id;
                DELETE FROM signing_route_template_stage WHERE id = stageId.id;
            END LOOP;
        UPDATE signing_route_template SET version = version + 1 WHERE id = templateId;
END
$$;