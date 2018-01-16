# coding: utf-8
"""
    update_date()
    Actualización de fechas de metadatos final_period_id, data_update y
    last_updated

    Date:
        27/10/2017

    Author:
        goi9999

    Version:
        1.0 (pre-launch)

    Notes:


"""
import sqlalchemy

from etlstat.log.timing import log
from etlstat.database.MySql import MySql

def update_date(config):
    """
    Actualiza las fechas del cubo/s en la base de datos de metadatos.

    Args:
        config:         Objeto Configuration con los parámetros necesarios para la
                        actualización de metadatos.

                        Formato de config_global:
                        -------------------------
                        CONFIG_GLOBAL = {
                           'metadata': {
                                'local': {
                                    'user': METADATA_USER_LOCAL,
                                    'password': METADATA_PASSWD_LOCAL
                                },
                                'dev': {
                                    'user': METADATA_USER_DEV,
                                    'password': METADATA_PASSWD_DEV
                                },
                                'pro': {
                                    'user': METADATA_USER_PRO,
                                    'password': METADATA_PASSWD_PRO
                                }
                            },
                            ...

                        Formato de icane_config:
                        ------------------
                            icane_config.environment = Env.LOCAL

                            CONFIG_LOCAL = {
                                'metadata': {
                                    'anno': 2017,
                                    'mes': 10,
                                    # [uri_tag puede ser una lista de uris o un único string]
                                    'uri_tag': ['cadastre-other-stats-constructions-antiquity-municipality-monthly',
                                                'cadastre-other-stats-categories-municipality-period-monthly',
                                                'cadastre-other-stats-distribution-uses-municipality-period-monthly',
                                                'cadastre-other-stats-vacant-land-municipality-period-monthly'],
                                    # [category_tag y subsection_tag son parámetros opcionales]
                                    'category_tag': 'municipal-data',
                                    'subsection_tag': ['construction-housing','territory']
                                }
                                ...
                            }
    Return:
        True si se actualizó con éxito.
        False en caso contrario.

    """
    answer = False
    table = 'time_series'
    table_select = ''
    params = 'final_period_id'
    params_select = ''
    conditions = ''
    conditions_select = ''
    connector, conn_data = config.store.metadata.split('//')
    username, link, port_db = conn_data.split(':')
    port, void = port_db.split('/')
    password, ip = link.split('@')
    database = 'metadata'
    num_uri = 1

    try:
        if connector == 'mysql+mysqlconnector:':
            conn = MySql(ip, port, database, username, password)
            if conn.state == False:
                raise sqlalchemy.exc.SQLAlchemyError("Can't connect to MySQL Database")
        else:
            raise NotImplementedError("Not yet!")

        # select 1: id_period
        table_select = 'time_period'
        params_select = 'id'
        conditions_select = '(start_month, start_year) = ({0}, {1})'\
                            .format(config.metadata.mes, config.metadata.anno)

        row_proxy = conn.select(table_select, params_select, conditions_select)

        if row_proxy is not None:
            if row_proxy.rowcount == 1:
                id_period = row_proxy.first()[0]
            elif row_proxy.rowcount == 0:
                row_proxy.close()
                raise sqlalchemy.exc.NoReferenceError(
                    'No existe coincidencia para time_period.id')
        else:
            raise sqlalchemy.exc.NoReferenceError(
                'No se ha podido realizar la consulta de time_period.id ({0},{1})'
                .format(config.metadata.mes, config.metadata.anno))

        params += ' = {0}, data_update = current_date(), last_updated = current_date()'\
                  .format(id_period)

        conditions += 'uri_tag in ('
        if isinstance(config.metadata.uri_tag, list):
            num_uri = len(config.metadata.uri_tag)
            for tag in config.metadata.uri_tag:
                conditions += "'{0}',".format(tag)
        else:
            conditions += "'{0}',".format(config.metadata.uri_tag)

        conditions = conditions[:-1]
        conditions += ')'

        if 'category_tag' in config.metadata.keys() or 'category_tag' in config.metadata.__dict__:
            # select 2: id_category
            table_select = 'category'
            params_select = 'id'
            conditions_select = "uri_tag in ('{0}')" \
                                .format(config.metadata.category_tag)

            row_proxy = conn.select(table_select, params_select, conditions_select)

            if row_proxy is not None:
                if row_proxy.rowcount == 1:
                    id_category = row_proxy.first()[0]
                else:
                    row_proxy.close()
                    raise sqlalchemy.exc.NoReferenceError(
                        'No existe coincidencia para category.id')
            else:
                raise sqlalchemy.exc.NoReferenceError(
                    'No existe coincidencia para category.id')

            conditions += ' and category_id = {0}'.format(id_category)

            if 'subsection_tag' in config.metadata.keys() or 'subsection_tag' in config.metadata.__dict__:
                # select 3: id_subsection (multiple)
                table_select = 'subsection'
                params_select = 'id'
                conditions_select = "uri_tag in ("

                if isinstance(config.metadata.subsection_tag, list):
                    for tag in config.metadata.subsection_tag:
                        conditions_select += "'{0}',".format(tag)
                    conditions_select = conditions_select[:-1]
                    conditions_select += ')'
                else:
                    conditions_select += "'{0}')".format(config.metadata.subsection_tag)

                row_proxy = conn.select(table_select, params_select,
                                        conditions_select)

                if row_proxy is not None:
                    if row_proxy.rowcount >= 1:
                        all_id = row_proxy.fetchall()
                    else:
                        row_proxy.close()
                        raise sqlalchemy.exc.NoReferenceError(
                              'No existe coincidencia para subsection.id')
                else:
                    raise sqlalchemy.exc.NoReferenceError(
                        'No existe coincidencia para subsection.id')

                id_subsection_list = '('
                for id in all_id:
                    for i in id:
                        id_subsection_list += "{0},".format(i)
                id_subsection_list = id_subsection_list[:-1]
                id_subsection_list += ')'

                conditions += ' and subsection_id in {0}'.format(id_subsection_list)

        result = conn.update(table, params, conditions)

        if result.rowcount >= num_uri:
            answer = True

    except sqlalchemy.exc.SQLAlchemyError as e:
        log.error(" {0}".format(e))
    except sqlalchemy.exc.IntegrityError as e:
        log.error(" {0}".format(e))
    except sqlalchemy.exc.NoReferenceError as e:
        log.error(" {0}".format(e))
    except KeyError:
        log.error(" La configuración recibida como parámetro no sigue el formato requerido.")

    return answer