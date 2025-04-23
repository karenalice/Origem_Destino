import os
import pyodbc
import pandas as pd

# Informações de conexão ao banco de dados Progress OpenEdge
server_name = 'seu servidor
database_name = 'nome do DB'
user_id = 'seu usuario'
password = 'sua senha'
port = 'porta'  # Porta do servidor

# String de conexão ODBC ajustada
connection_string = (
    f'DRIVER={{Progress OpenEdge 12.2 driver}};'
    f'HOST={server_name};'
    f'PORT={port};'
    f'DATABASE={database_name};'
    f'UID={user_id};'
    f'PWD={password};'
)

try:
    # Etapa 1: Realizar SELECT no banco
    connection = pyodbc.connect(connection_string)
    print("Conexão ao banco de dados bem-sucedida!")

    sql_query = """
    SELECT 
        pub.bilhete.codcia AS CIA,
        pub.bilhete.numtkt AS BILHETE,
        pub.bilhete.loc AS PNR_AGENCIA,
        pub.itinbilh.aerporig AS ORIGEM,
        pub.itinbilh.aerpdest AS DESTINO,
        pub.itinbilh.ordem AS CUPOM,
        pub.itinbilh.basetrf AS BASE_TARIFARIA,
        pub.itinbilh.horaorig AS HORA_EMBARQUE,
        pub.itinbilh.horadest AS HORA_POUSO,
        pub.itinbilh.data AS DATA_EMBARQUE,
        pub.bilhete.dataemi AS DATA_EMISSAO,
        pub.bilhete.tarifam AS TARIFA,
        pub.bilhete.iata AS IATA,
        pub.bilhete.codest AS base,
        pub.bilhete.gov AS GOV,
        pub.venda.data,
        pub.bilhete.cambio AS CAMBIO
    FROM 
        pub.bilhete 
    JOIN 
        pub.venda 
        ON (pub.bilhete.codest = pub.venda.codest AND pub.bilhete.numvend = pub.venda.numvend)
    LEFT JOIN
        pub.itinbilh 
        ON pub.bilhete.numtkt = pub.itinbilh.numtkt
    WHERE
        pub.bilhete.dataemi >= '2025-01-01' 
        AND pub.bilhete.dataemi <= '2025-04-15' 
        AND pub.venda.cancelado = 0
        AND pub.bilhete.codfor ='1073'
        AND pub.bilhete.tipo ='I'
        AND (pub.bilhete.codcia = 'AF' OR pub.bilhete.codcia = 'KL') 
    """
        #AND pub.bilhete.codcia <> 'JJ'
        #AND pub.bilhete.codcia IN ('AA', 'JJ', 'CM', 'UA', 'AC', 'DL')
        #AND pub.bilhete.codcia = 'AF'
        #AND (pub.bilhete.codcia = 'AF' OR pub.bilhete.codcia = 'KL')
        #AND pub.bilhete.numtkt ='0571408370139'
        #AND pub.bilhete.loc ='TIEODW'
    df = pd.read_sql(sql_query, connection)

    print(df.head())
    print('DADOS',len(df) )
    
    def preencher_bilhetes_vazios(df):
        bilhetes_corrigidos = []

        for pnr in df['PNR_AGENCIA'].unique():
            pnr_df = df[df['PNR_AGENCIA'] == pnr]

            # Identifica bilhetes com erro
            bilhetes_com_erro = pnr_df.groupby('BILHETE').filter(
                lambda x: x['ORIGEM'].isna().any() or x['DESTINO'].isna().any()
            )['BILHETE'].unique()

            # Identifica bilhetes válidos
            bilhetes_validos = pnr_df.groupby('BILHETE').filter(
                lambda x: x['ORIGEM'].notna().all() and x['DESTINO'].notna().all() and x['HORA_POUSO'].notna().all()
            )

            for bilhete in bilhetes_com_erro:
                bilhete_com_erro_df = pnr_df[pnr_df['BILHETE'] == bilhete]

                if not bilhetes_validos.empty:
                    # Pega o primeiro bilhete válido com todos os cupons
                    bilhete_ref = bilhetes_validos.groupby('BILHETE').first().index[0]
                    bilhete_ref_df = bilhetes_validos[bilhetes_validos['BILHETE'] == bilhete_ref]

                    for _, cupom_ref in bilhete_ref_df.iterrows():
                        cupom_corrigido = cupom_ref.copy()
                        cupom_corrigido['BILHETE'] = bilhete  # usa o bilhete original com erro
                        cupom_corrigido['TARIFA'] = bilhete_com_erro_df['TARIFA'].iloc[0]
                        cupom_corrigido['CAMBIO'] = bilhete_com_erro_df['CAMBIO'].iloc[0]
                        cupom_corrigido['TARIFA_BRL'] = cupom_corrigido['TARIFA'] * cupom_corrigido['CAMBIO']
                        cupom_corrigido['OBS'] = "CORRIGIDO COM TODOS OS CUPONS DE UM BILHETE DO MESMO PNR"
                        bilhetes_corrigidos.append(cupom_corrigido)
                else:
                    # Preenchimento padrão caso não haja referência
                    for _, row in bilhete_com_erro_df.iterrows():
                        corrigido = row.copy()
                        corrigido['ORIGEM'] = 'ORIGEM 1'
                        corrigido['DESTINO'] = 'DESTINO 1'
                        corrigido['OBS'] = 'SEM REFERÊNCIA NO PNR - PADRÃO'
                        corrigido['TARIFA_BRL'] = corrigido['TARIFA'] * corrigido['CAMBIO']
                        bilhetes_corrigidos.append(corrigido)

                # Remove o bilhete original com erro
                df = df[~((df['PNR_AGENCIA'] == pnr) & (df['BILHETE'] == bilhete))]

        # Junta os corrigidos com o restante
        df = pd.concat([df, pd.DataFrame(bilhetes_corrigidos)], ignore_index=True)
        return df



    # Aplicação da função ajustada ao DataFrame:
    df = preencher_bilhetes_vazios(df)
    print(df)      
    
    def remover_linhas_repetidas_sequenciais(bilhete_df):
        indices_para_remover = []

        for i in range(1, len(bilhete_df)):
            # ORIGEM igual à anterior ➜ desconsiderar o anterior (i-1)
            if bilhete_df['ORIGEM'].iloc[i] == bilhete_df['ORIGEM'].iloc[i-1]:
                indices_para_remover.append(bilhete_df.index[i-1])
            
            # DESTINO igual à anterior ➜ desconsiderar o anterior (i-1)
            elif bilhete_df['DESTINO'].iloc[i] == bilhete_df['DESTINO'].iloc[i-1]:
                indices_para_remover.append(bilhete_df.index[i-1])

        # Remove as linhas marcadas
        bilhete_df = bilhete_df.drop(index=indices_para_remover)
        return bilhete_df
    
    df = remover_linhas_repetidas_sequenciais(df)
    print('linhas seq',df) 
     
    def formatar_hora(hora):
        """ Formata a hora no formato HH:MM, lidando com valores inválidos. """
        if pd.isnull(hora) or str(hora).strip() == "":
            return "00:00"  # Valor vazio ou em branco
        try:
            if isinstance(hora, pd.Timestamp):  # Se for Timestamp, converte para string HH:MM
                return hora.strftime('%H:%M')

            hora = str(hora)  # Converte qualquer outro tipo para string
            
            if hora == "23:59:59":
                return "23:59"

            # Se a hora já estiver no formato HH:MM, mantém
            if ":" in hora and len(hora) == 5:
                horas, minutos = map(int, hora.split(":"))
            else:
                # Se a hora for "0938", extrai os dois primeiros como horas e os dois últimos como minutos
                hora = f"{int(hora):04d}"  # Garante formato de 4 dígitos
                horas, minutos = int(hora[:2]), int(hora[2:])
            
            # Validação do intervalo de horas e minutos
            if 0 <= horas <= 23 and 0 <= minutos <= 59:
                return f"{horas:02d}:{minutos:02d}"
            else:
                return "00:00"  # Valor inválido
        except ValueError:
            return "00:00"  # Valor inválido

    # Aplicar a formatação nas colunas de horas
    for coluna_hora in ['HORA_EMBARQUE', 'HORA_POUSO']:
        df[coluna_hora] = df[coluna_hora].fillna("00:00").apply(formatar_hora)

    # Garantir que as horas estejam no formato datetime
    for coluna_hora in ['HORA_EMBARQUE', 'HORA_POUSO']:
        df[coluna_hora] = pd.to_datetime(df[coluna_hora], format='%H:%M', errors='coerce')
        # Aplicar a formatação nas colunas de horas
    for coluna_hora in ['HORA_EMBARQUE', 'HORA_POUSO']:
        df[coluna_hora] = df[coluna_hora].fillna(pd.NaT).apply(formatar_hora)

    # Garantir que as horas estejam no formato datetime
    for coluna_hora in ['HORA_EMBARQUE', 'HORA_POUSO']:
        df[coluna_hora] = pd.to_datetime(df[coluna_hora], format='%H:%M', errors='coerce')


    # Função para substituir DATA_EMBARQUE vazia pelo mesmo PNR ou pela DATA_EMISSAO
    def preencher_data_embarque(df):
        """ Se DATA_EMBARQUE estiver vazia, substitui pela data de outro bilhete com o mesmo PNR.
            Se não houver outro bilhete, usa a DATA_EMISSAO como fallback. """
        
        df.sort_values(by=['PNR_AGENCIA', 'CUPOM'], inplace=True)  # Ordena para facilitar a busca
        
        for pnr in df['PNR_AGENCIA'].unique():
            pnr_df = df[df['PNR_AGENCIA'] == pnr]

            for cupom in pnr_df['CUPOM'].unique():
                # Filtra os bilhetes com o mesmo PNR e CUPOM
                cupom_df = pnr_df[pnr_df['CUPOM'] == cupom]
                
                # Verifica se há algum bilhete com DATA_EMBARQUE preenchida
                bilhete_valido = cupom_df[cupom_df['DATA_EMBARQUE'].notna() & (cupom_df['DATA_EMBARQUE'] != "")]
                
                if not bilhete_valido.empty:
                    data_referencia = bilhete_valido['DATA_EMBARQUE'].iloc[0]
                else:
                    # Caso não encontre outro bilhete, usa a DATA_EMISSAO como referência
                    data_referencia = cupom_df['DATA_EMISSAO'].iloc[0] if not cupom_df['DATA_EMISSAO'].isna().all() else None
                
                # Atualiza os bilhetes com DATA_EMBARQUE vazia
                df.loc[
                    (df['PNR_AGENCIA'] == pnr) & (df['CUPOM'] == cupom) & 
                    (df['DATA_EMBARQUE'].isna() | (df['DATA_EMBARQUE'] == "")), 
                    'DATA_EMBARQUE'
                ] = data_referencia

        return df

    # Aplicar o preenchimento de datas vazias
    df = preencher_data_embarque(df)
    #print(df)

    # Etapa 3: Processar bilhetes com apenas 1 cupom
    bilhetes_1_cupom = []
    restantes_para_contagem = []

    for pnr in df['PNR_AGENCIA'].unique():
        pnr_df = df[df['PNR_AGENCIA'] == pnr]

        for bilhete in pnr_df['BILHETE'].unique():
            bilhete_df = pnr_df[pnr_df['BILHETE'] == bilhete]

            if len(bilhete_df) == 1:
                bilhetes_1_cupom.append([
                    bilhete_df['BASE'].iloc[0],
                    bilhete_df['GOV'].iloc[0],
                    bilhete_df['IATA'].iloc[0],
                    bilhete_df['BILHETE'].iloc[0],
                    bilhete_df['CIA'].iloc[0],
                    bilhete_df['ORIGEM'].iloc[0],
                    bilhete_df['DESTINO'].iloc[0],
                    bilhete_df['DATA_EMISSAO'].iloc[0],
                    bilhete_df['TARIFA'].iloc[0] * bilhete_df['CAMBIO'].iloc[0],
                    pnr,
                    bilhete_df['DATA_EMBARQUE'].iloc[0],
                    'APENAS 1 CUPOM'
                ])
            else:
                restantes_para_contagem.append(bilhete_df)

    bilhetes_1_cupom_df = pd.DataFrame(bilhetes_1_cupom, columns=['BASE', 'GOV', 'IATA', 'BILHETE', 'CIA', 'ORIGEM', 'DESTINO', 'DATA_EMISSAO', 'TARIFA_BRL', 'PNR_AGENCIA', 'DATA_EMBARQUE', 'OBS'])
    print(f"Resultado 1 CUPONS: {len(bilhetes_1_cupom_df )} registros")

    # Etapa 4: Processar bilhetes com 2 cupons (ajustado)
    bilhetes_2_cupons = []
    df_restante = pd.concat(restantes_para_contagem, ignore_index=True)

    for pnr in df_restante['PNR_AGENCIA'].unique():
        pnr_df = df_restante[df_restante['PNR_AGENCIA'] == pnr]

        for bilhete in pnr_df['BILHETE'].unique():
            bilhete_df = pnr_df[pnr_df['BILHETE'] == bilhete]

            if len(bilhete_df) == 2:
                # Conversão de data e hora
                data_embarque_1 = pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[0], dayfirst=True)
                data_embarque_2 = pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[1], dayfirst=True)

                diferenca_dias = abs((data_embarque_2 - data_embarque_1).days)
                #print('diferença de dias ', diferenca_dias)
                
                # Acessando o primeiro valor de HORA_POUSO
                hora_pouso_1 = bilhete_df['HORA_POUSO'].iloc[0]
                #print('hora_pouso:', hora_pouso_1)

                # Acessando o segundo valor de HORA_EMBARQUE
                hora_embarque_2 = bilhete_df['HORA_EMBARQUE'].iloc[1]
               # print('hora_embarque:', hora_embarque_2)

                
                diferenca_horas = (hora_embarque_2 - hora_pouso_1).total_seconds() / 3600 if pd.notnull(hora_pouso_1) and pd.notnull(hora_embarque_2) else 0
                #print('diferença de horas ', diferenca_horas)
                ultimo_destino = bilhete_df['DESTINO'].iloc[1]
                origem = bilhete_df['ORIGEM'].iloc[0]
                # Critérios para definir o destino final
                
                if origem != ultimo_destino:
                    if diferenca_dias > 1:
                        destino_final = bilhete_df['DESTINO'].iloc[0]
                    else:    
                        destino_final = bilhete_df['DESTINO'].iloc[1]
                else:
                        destino_final = bilhete_df['DESTINO'].iloc[0]

                bilhetes_2_cupons.append([
                    bilhete_df['BASE'].iloc[0],
                    bilhete_df['GOV'].iloc[0],
                    bilhete_df['IATA'].iloc[0],
                    bilhete_df['BILHETE'].iloc[0],
                    bilhete_df['CIA'].iloc[0],
                    bilhete_df['ORIGEM'].iloc[0],
                    destino_final,
                    bilhete_df['DATA_EMISSAO'].iloc[0],
                    bilhete_df['TARIFA'].iloc[0] * bilhete_df['CAMBIO'].iloc[0],
                    pnr,
                    bilhete_df['DATA_EMBARQUE'].iloc[0],
                    '2 CUPONS'
                ])
        else:
            # Caso não entre como cupom 1 ou cupom 2
            restantes_para_contagem.append(bilhete_df)

    bilhetes_2_cupons_df = pd.DataFrame(bilhetes_2_cupons, columns=['BASE', 'GOV', 'IATA', 'BILHETE', 'CIA', 'ORIGEM', 'DESTINO', 'DATA_EMISSAO', 'TARIFA_BRL', 'PNR_AGENCIA', 'DATA_EMBARQUE', 'OBS'])
    print(f"Resultado 2 CUPONS: {len(bilhetes_2_cupons_df)}")
    
    # Continuar o fluxo para contagem de paradas (restantes)
    # Excluir bilhetes processados anteriormente antes de stopover
    bilhetes_processados = pd.concat([bilhetes_1_cupom_df, bilhetes_2_cupons_df], ignore_index=True)
    print(f"bilhetes_processados: {len(bilhetes_processados)} registros")
    
    df_restante = df_restante[~df_restante['BILHETE'].isin(bilhetes_processados['BILHETE'])] 
    print(f"df_restante: {len(df_restante)} registros")
    
    # Contador de paradas
    pnr_paradas = {}

    # Iterar sobre cada PNR no DataFrame restante
    for pnr in df_restante['PNR_AGENCIA'].unique():
        pnr_df = df_restante[df_restante['PNR_AGENCIA'] == pnr]
        total_paradas_pnr = 0  # Inicializar o contador de paradas para o PNR atual

        # Iterar sobre cada bilhete dentro do PNR
        for bilhete in pnr_df['BILHETE'].unique():
            bilhete_df = pnr_df[pnr_df['BILHETE'] == bilhete].sort_values(by=['CUPOM'])  # Ordenar cupons por ordem
            paradas_bilhete = 0  # Inicializar contador de paradas para o bilhete

            # Iterar sobre os cupons do bilhete
            for i in range(1, len(bilhete_df)):
                # Obter data de embarque anterior e atual
                data_embarque_anterior = pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[i - 1], dayfirst=True)
                data_embarque_atual = pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[i], dayfirst=True)

                if data_embarque_atual is not None and data_embarque_anterior is not None:
                    diferenca_dias = abs((data_embarque_atual - data_embarque_anterior).days)
                else:
                    diferenca_dias = 0  # defina aqui o valor que fizer sentido para sua aplicação

                # Obter hora de pouso anterior e hora de embarque atual
                hora_pouso_anterior = bilhete_df['HORA_POUSO'].iloc[i - 1]
                hora_embarque_atual = bilhete_df['HORA_EMBARQUE'].iloc[i]

                # Calcular a diferença de horas, se as horas forem válidas
                if pd.notnull(hora_pouso_anterior) and pd.notnull(hora_embarque_atual):
                    diferenca_horas = (hora_embarque_atual - hora_pouso_anterior).total_seconds() / 3600
                else:
                    diferenca_horas = 0  # Caso uma das horas seja inválida

                # Determinar se há uma parada
                if diferenca_dias == 1 and diferenca_horas < 16:
                    paradas_bilhete += 0 # Não incrementa o contador de paradas
                    #print(f"PNR: {pnr}, Bilhete: {bilhete}, Cupom {i} -> Sem parada (Diferença Dias: {diferenca_dias}, Horas: {diferenca_horas:.2f})")
                elif diferenca_dias > 1 or diferenca_horas >= 16:
                    paradas_bilhete += 1  # Incrementa o contador de paradas
                    #print(f"PNR: {pnr}, Bilhete: {bilhete}, Cupom {i} -> Parada detectada (Diferença Dias: {diferenca_dias}, Horas: {diferenca_horas:.2f})")
                    
                if i == len(bilhete_df) - 1:
                    break  # Sair do loop de cupons para o próximo bilhete
                # Adicionar as paradas do bilhete ao total de paradas do PNR
                total_paradas_pnr += paradas_bilhete

        # Salvar o total de paradas para o PNR atual
        pnr_paradas[pnr] = paradas_bilhete
        #print(f"Total de paradas para o PNR {pnr}: {paradas_bilhete}")

        
    # Converter o dicionário de paradas para um DataFrame
    pnr_paradas_df = pd.DataFrame(list(pnr_paradas.items()), columns=['PNR_AGENCIA', 'TOTAL_PARADAS'])

    # Separar PNRs com mais de duas paradas
    pnr_com_mais_de_duas_paradas = pnr_paradas_df[pnr_paradas_df['TOTAL_PARADAS'] > 2]['PNR_AGENCIA']

    # Criar DataFrame para stopover e restante
    df_stopover = df_restante[df_restante['PNR_AGENCIA'].isin(pnr_com_mais_de_duas_paradas)]
    df_restante = df_restante[~df_restante['PNR_AGENCIA'].isin(pnr_com_mais_de_duas_paradas)]

    # Exibir resultados
    print(f"DataFrame Stopover: {len(df_stopover)}registros")
    print(f"DataFrame Restante: {len(df_restante)} registros")
    
    # Etapa 5: Processar stopover
    resultado_stopover = []

    # Garantir que apenas `df_stopover` seja processado
    for pnr in df_stopover['PNR_AGENCIA'].unique():
        pnr_df = df_stopover[df_stopover['PNR_AGENCIA'] == pnr].reset_index(drop=True)
        origem = pnr_df['ORIGEM'].iloc[0]
        destino_final = pnr_df['DESTINO'].iloc[-1]  # inicialização padrão

        stopover_detectado = False
        base_tarifaria_mudou = False

        bases_tarifarias = pnr_df['BASE_TARIFARIA'].unique()

        for i in range(1, len(pnr_df)):
            data_embarque_anterior = pd.to_datetime(pnr_df['DATA_EMBARQUE'].iloc[i - 1], dayfirst=True)
            data_embarque_atual = pd.to_datetime(pnr_df['DATA_EMBARQUE'].iloc[i], dayfirst=True)

            diferenca_dias = (data_embarque_atual - data_embarque_anterior).days

            hora_pouso_anterior = pnr_df['HORA_POUSO'].iloc[i - 1]
            hora_embarque_atual = pnr_df['HORA_EMBARQUE'].iloc[i]

            diferenca_horas = abs((hora_embarque_atual - hora_pouso_anterior).total_seconds() / 3600) if pd.notnull(hora_pouso_anterior) and pd.notnull(hora_embarque_atual) else 0

            # Detectar stopover com regra original (mantida)
            if diferenca_horas > 14 and diferenca_dias > 1:
                stopover_detectado = True
            elif diferenca_horas <= 14 and diferenca_dias <= 1:
                stopover_detectado = False

            # Verificar se houve mudança na base tarifária
            if pnr_df['BASE_TARIFARIA'].iloc[i - 1] != pnr_df['BASE_TARIFARIA'].iloc[i]:
                base_tarifaria_mudou = True
                destino_final = pnr_df['DESTINO'].iloc[i - 1]  # Último destino antes da mudança
                break

        # Se NÃO houve mudança de base tarifária, definir destino como penúltima parada
        if not base_tarifaria_mudou:
            if len(pnr_df) > 1:
                destino_final = pnr_df['DESTINO'].iloc[-2]  # Penúltima parada
            else:
                destino_final = pnr_df['DESTINO'].iloc[-1]  # Caso só tenha um cupom

        # Adicionar ao resultado de "stopover" apenas se detectado
        if stopover_detectado:
            resultado_stopover.append([
                pnr_df['BASE'].iloc[0],
                pnr_df['GOV'].iloc[0],
                pnr_df['IATA'].iloc[0],
                pnr_df['BILHETE'].iloc[0],
                pnr_df['CIA'].iloc[0],
                origem,
                destino_final,
                pnr_df['DATA_EMISSAO'].iloc[0],
                pnr_df['TARIFA'].iloc[0] * pnr_df['CAMBIO'].iloc[0],
                pnr,
                pnr_df['DATA_EMBARQUE'].iloc[0],
                'STOPOVER'
            ])
        else:
            # Caso não seja um stopover, adicionar ao restante
            df_restante = pd.concat([df_restante, pnr_df], ignore_index=True)


    # Converter o resultado para DataFrame
    resultado_stopover_df = pd.DataFrame(resultado_stopover, columns=['BASE', 'GOV', 'IATA', 'BILHETE', 'CIA', 'ORIGEM', 'DESTINO', 'DATA_EMISSAO', 'TARIFA_BRL', 'PNR_AGENCIA', 'DATA_EMBARQUE', 'OBS'])
    print(f"Resultado STOPOVER: {len(resultado_stopover_df)} registros")


    # Converter o resultado para DataFrame
    resultado_stopover_df = pd.DataFrame(resultado_stopover, columns=['BASE', 'GOV', 'IATA', 'BILHETE', 'CIA', 'ORIGEM', 'DESTINO', 'DATA_EMISSAO', 'TARIFA_BRL', 'PNR_AGENCIA', 'DATA_EMBARQUE', 'OBS'])
    print(f"Resultado STOPOVER: {len(resultado_stopover_df)} registros")

    # Recuperar todos os bilhetes do DataFrame original
    bilhetes_todos = set(df['BILHETE'])

    # Recuperar bilhetes já processados nas etapas 1, 2 e stopover
    bilhetes_processados_iniciais = set(
        bilhetes_1_cupom_df['BILHETE']
    ).union(
        bilhetes_2_cupons_df['BILHETE']
    ).union(
        resultado_stopover_df['BILHETE']
    )

    # Identificar bilhetes que ainda não foram processados
    bilhetes_nao_processados_iniciais = bilhetes_todos - bilhetes_processados_iniciais

    # Se houver bilhetes ainda não processados, adicioná-los ao df_restante para a próxima etapa
    if bilhetes_nao_processados_iniciais:
        print(f"🔎 Bilhetes a serem processados nas etapas seguintes: {len(bilhetes_nao_processados_iniciais)}")
        df_nao_processados = df[df['BILHETE'].isin(bilhetes_nao_processados_iniciais)].copy()
        df_restante = pd.concat([df_restante, df_nao_processados], ignore_index=True)

    # Etapa 6: Processar restante com identificação simples
        resultado_restante = []

        for pnr in df_restante['PNR_AGENCIA'].unique():
            pnr_df = df_restante[df_restante['PNR_AGENCIA'] == pnr]
            origem = pnr_df['ORIGEM'].iloc[0]
            destino_final = origem
            
            
            if len(pnr_df) < 2:
                continue

            for i in range(1, len(pnr_df)):
                hora_pouso_anterior = pd.to_datetime(pnr_df['HORA_POUSO'].iloc[i - 1])
                hora_embarque_atual = pd.to_datetime(pnr_df['HORA_EMBARQUE'].iloc[i])
                diferenca_horas = (hora_embarque_atual - hora_pouso_anterior).total_seconds() / 3600

                data_anterior = pd.to_datetime(pnr_df['DATA_EMBARQUE'].iloc[i - 1], dayfirst=True)
                data_atual = pd.to_datetime(pnr_df['DATA_EMBARQUE'].iloc[i], dayfirst=True)
                diferenca_dias = abs((data_atual - data_anterior).days)
                
                    # ➤ NOVA CONDIÇÃO: Se a diferença de horas for negativa ou zero
                if diferenca_horas <= 0:
                    diferenca_negativa_detectada = True

                    # ➤ Dentro dessa condição, se encontrar diferença de dias > 1, esse é o destino final
                    if len(bilhete_df) > i - 1:
                        destino_final = bilhete_df['DESTINO'].iloc[i - 1]
                    else:
                        destino_final = bilhete_df['DESTINO'].iloc[0]  # fallback seguro
                        break
                    
                    
                    if len(bilhete_df) == 4:
                        ultimo_destino_cupomS = bilhete_df['DESTINO'].iloc[3]
                        for i in range(len(bilhete_df)):
                            
                            if origem == ultimo_destino_cupomS and abs((bilhete_df['DATA_EMBARQUE'].iloc[2] - bilhete_df['DATA_EMBARQUE'].iloc[1]).days) == 1:
                                destino_final = bilhete_df['DESTINO'].iloc[1]
                                
                            if origem == ultimo_destino_cupomS and abs((bilhete_df['DATA_EMBARQUE'].iloc[2] - bilhete_df['DATA_EMBARQUE'].iloc[1]).days) > 1:
                                destino_final = bilhete_df['DESTINO'].iloc[0]
                            
                            elif (
                                abs((bilhete_df['DATA_EMBARQUE'].iloc[1] - bilhete_df['DATA_EMBARQUE'].iloc[0]).days) == 0 and
                                abs((bilhete_df['DATA_EMBARQUE'].iloc[2] - bilhete_df['DATA_EMBARQUE'].iloc[1]).days) == 1
                                ):
                                destino_final = bilhete_df['DESTINO'].iloc[2]
                                break
                            elif (
                                abs((bilhete_df['DATA_EMBARQUE'].iloc[1] - bilhete_df['DATA_EMBARQUE'].iloc[0]).days) == 0 and
                                abs((bilhete_df['DATA_EMBARQUE'].iloc[2] - bilhete_df['DATA_EMBARQUE'].iloc[1]).days) == 0
                                ):
                                destino_final = bilhete_df['DESTINO'].iloc[2]
                                break
                            elif (
                                abs((bilhete_df['DATA_EMBARQUE'].iloc[1] - bilhete_df['DATA_EMBARQUE'].iloc[0]).days) == 1 and
                                abs((bilhete_df['DATA_EMBARQUE'].iloc[2] - bilhete_df['DATA_EMBARQUE'].iloc[1]).days) == 0
                                ):
                                destino_final = bilhete_df['DESTINO'].iloc[2]
                                break
                            elif diferenca_dias == 0:
                                destino_final = bilhete_df['DESTINO'].iloc[1]   
                            elif diferenca_dias == 1:
                                destino_final = bilhete_df['DESTINO'].iloc[1]
                                #print("condicao 4",destino_final )

                        break    

                # Condição 2
                
                if diferenca_dias > 1:
                    destino_final = pnr_df['DESTINO'].iloc[i - 1]
                    # print(f"Condicional: 'diferenca_dias > 1'. Destino Final: {destino_final}")
                    break

                # Condição 3
                elif diferenca_dias >= 1 and diferenca_horas == 0 or diferenca_horas > 0:
                    destino_final = bilhete_df['DESTINO'].iloc[0]
                    # print(f"Condicional: 'diferenca_dias >= 1 and diferenca_horas == 0 or diferenca_horas > 0'. Destino Final: {destino_final}")

                # Condição 4
                elif len(bilhete_df) > 2 and diferenca_horas <= 0 and diferenca_dias > 2:
                    destino_final = bilhete_df['DESTINO'].iloc[i - 1]
                    # print(f"Condicional: 'len(bilhete_df) > 2 and diferenca_horas <= 0 and diferenca_dias > 2'. Destino Final: {destino_final}")

                # Condição 5
                elif diferenca_dias > 1 and diferenca_horas <= 5:
                    destino_final = bilhete_df['DESTINO'].iloc[i - 1]

                # Condição 1 (agora por último)
                elif diferenca_dias == 1 and diferenca_horas < 3:
                    if i < len(pnr_df):
                        destino_final = pnr_df['DESTINO'].iloc[i]
                        if destino_final == origem:
                            destino_final = pnr_df['DESTINO'].iloc[i - 1]
                        break

                # Fallback
                else:
                    destino_final = pnr_df['DESTINO'].iloc[i]

    # **ADICIONAR RESULTADOS DENTRO DO LOOP CORRETAMENTE**
        resultado_restante.append([
            pnr_df['BASE'].iloc[0],
            pnr_df['GOV'].iloc[0],
            pnr_df['IATA'].iloc[0],
            pnr_df['BILHETE'].iloc[0],
            pnr_df['CIA'].iloc[0],
            origem,
            destino_final,
            pnr_df['DATA_EMISSAO'].iloc[0],
            pnr_df['TARIFA'].iloc[0] * pnr_df['CAMBIO'].iloc[0],
            pnr,
            pnr_df['DATA_EMBARQUE'].iloc[0],
            'IDENTIFICAÇÃO SIMPLES'
        ])
        #print('resultado simples',resultado_restante)
        

    resultado_restante_df = pd.DataFrame(resultado_restante, columns=['BASE', 'GOV', 'IATA', 'BILHETE', 'CIA', 'ORIGEM', 'DESTINO', 'DATA_EMISSAO', 'TARIFA_BRL', 'PNR_AGENCIA', 'DATA_EMBARQUE', 'OBS'])
    #print(f"Resultado Restante: {resultado_restante_df} registros")

    # Combinar todos os resultados
    resultado_final = pd.concat([
        bilhetes_1_cupom_df,
        bilhetes_2_cupons_df,
        resultado_stopover_df,
        resultado_restante_df
    ], ignore_index=True)

    # Verificar bilhetes não processados
    bilhetes_todos = set(df['BILHETE'])
    bilhetes_processados = set(resultado_final['BILHETE'])
    bilhetes_nao_processados = bilhetes_todos - bilhetes_processados

    if bilhetes_nao_processados:
        print(f"Bilhetes não processados: {len(bilhetes_nao_processados)}")
        df_nao_processados = df[df['BILHETE'].isin(bilhetes_nao_processados)]
        resultado_final = pd.concat([resultado_final, df_nao_processados], ignore_index=True)

    print(f"Registros no resultado final: {len(resultado_final)} registros")

# Etapa 7: Reavaliação de registros com origem e destino iguais

    resultado_final = pd.concat([bilhetes_1_cupom_df, bilhetes_2_cupons_df, resultado_stopover_df, resultado_restante_df], ignore_index=True)
    
    reavaliar_df = resultado_final[resultado_final['ORIGEM'] == resultado_final['DESTINO']]
    elegiveis_df = resultado_final[resultado_final['ORIGEM'] != resultado_final['DESTINO']]

    # Adicionar bilhetes não processados diretamente aos elegíveis
    if 'df_nao_processados' in locals() and not df_nao_processados.empty:
        print(f"Adicionando {len(df_nao_processados)} registros não processados aos elegíveis.")
        reavaliar_df = pd.concat([reavaliar_df, df_nao_processados], ignore_index=True)

        # Imprimir estatísticas
    print(f"Quantidade de registros a reavaliar (origem = destino): {len(reavaliar_df)}")
    print(f"Quantidade de registros elegíveis (origem != destino): {len(elegiveis_df)}")

    reavaliados = []

# Reprocessar registros com origem e destino iguais
    for _, row in reavaliar_df.iterrows():
        # Extrair as informações do PNR e bilhete
        pnr = row['PNR_AGENCIA']
        bilhete = row['BILHETE']

        # Filtrar os dados correspondentes no DataFrame original para esse PNR e bilhete
        pnr_df = df[df['PNR_AGENCIA'] == pnr]
        bilhete_df = pnr_df[pnr_df['BILHETE'] == bilhete].sort_values(by=['CUPOM'])

        total_paradas_pnr = 0  # Contador de paradas para o PNR atual

        # Contar paradas no bilhete
        for i in range(1, len(bilhete_df)):
            # Obter informações de embarque e pouso
            data_embarque_anterior = pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[i - 1], dayfirst=True)
            data_embarque_atual = pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[i], dayfirst=True)
            if data_embarque_atual is not None and data_embarque_anterior is not None:
                diferenca_dias = abs((data_embarque_atual - data_embarque_anterior).days)
            else:
                diferenca_dias = 0  # defina aqui o valor que fizer sentido para sua aplicação


            hora_pouso_anterior = bilhete_df['HORA_POUSO'].iloc[i - 1]
            hora_embarque_atual = bilhete_df['HORA_EMBARQUE'].iloc[i]

            if pd.notnull(hora_pouso_anterior) and pd.notnull(hora_embarque_atual):
                diferenca_horas = (hora_embarque_atual - hora_pouso_anterior).total_seconds() / 3600
            else:
                diferenca_horas = 0  # Caso as horas sejam inválidas

            # Identificar paradas
            if diferenca_horas > 14 or diferenca_dias > 1:
                total_paradas_pnr += 1
            if diferenca_horas <= 14 and diferenca_dias <= 1:
                total_paradas_pnr += 0
        
            elif diferenca_horas <= 0 and len(bilhete_df) > 2:
                destinos = bilhete_df['DESTINO'].tolist()
                origens = bilhete_df['ORIGEM'].tolist()
                frequencias = pd.Series(destinos + origens).value_counts()
                destino_menos_frequente = frequencias.idxmin()
                total_paradas_pnr += 1
                #print ('TOTAL PARADA',total_paradas_pnr )
                    
        # Prioridade para bilhetes com 1 ou 2 cupons
        if len(bilhete_df) == 1:
            # Caso de apenas 1 cupom
            reavaliados.append([
                bilhete_df['BASE'].iloc[0],
                bilhete_df['GOV'].iloc[0],
                bilhete_df['IATA'].iloc[0],
                bilhete_df['BILHETE'].iloc[0],
                bilhete_df['CIA'].iloc[0],
                bilhete_df['ORIGEM'].iloc[0],
                bilhete_df['DESTINO'].iloc[0],
                bilhete_df['DATA_EMISSAO'].iloc[0],
                bilhete_df['TARIFA'].iloc[0] * bilhete_df['CAMBIO'].iloc[0],
                pnr,
                bilhete_df['DATA_EMBARQUE'].iloc[0],
                'APENAS 1 CUPOM (REPROCESSADO)'
            ])
        elif len(bilhete_df) == 2:
            # Caso de 2 cupons
            data_embarque_1 = pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[0], dayfirst=True)
            data_embarque_2 = pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[1], dayfirst=True)

            diferenca_dias = (data_embarque_2 - data_embarque_1).days
            # Acessando o primeiro valor de HORA_POUSO
            
            hora_pouso_1 = bilhete_df['HORA_POUSO'].iloc[0]
            #print('hora_pouso:', hora_pouso_1)

            # Acessando o segundo valor de HORA_EMBARQUE
            hora_embarque_2 = bilhete_df['HORA_EMBARQUE'].iloc[1]
            # print('hora_embarque:', hora_embarque_2)

            diferenca_horas = (hora_embarque_2 - hora_pouso_1).total_seconds() / 3600 if pd.notnull(hora_pouso_1) and pd.notnull(hora_embarque_2) else 0
           
            ultimo_destino = bilhete_df['DESTINO'].iloc[1]
            origem = bilhete_df['ORIGEM'].iloc[0]
                
            if diferenca_dias > 1:
                estino_final = bilhete_df['DESTINO'].iloc[0]
            elif origem != ultimo_destino:
                destino_final = bilhete_df['DESTINO'].iloc[1]
            elif origem == ultimo_destino:
                destino_final = bilhete_df['DESTINO'].iloc[0]
            elif diferenca_dias == 0 and diferenca_horas >= 12:
                destino_final = bilhete_df['DESTINO'].iloc[0]
            elif diferenca_dias >= 1 or diferenca_horas >= 5:
                destino_final = bilhete_df['DESTINO'].iloc[0]
            elif diferenca_horas == 0 or  diferenca_horas > 0:
                destino_final = bilhete_df['DESTINO'].iloc[0]
            elif diferenca_dias <= 0 and diferenca_horas < 12:
                destino_final = bilhete_df['DESTINO'].iloc[1]
                if destino_final == origem:
                    destino_final = bilhete_df['DESTINO'].iloc[0]
                    break
            else:
                destino_final = bilhete_df['DESTINO'].iloc[1]

            reavaliados.append([
                bilhete_df['BASE'].iloc[0],
                bilhete_df['GOV'].iloc[0],
                bilhete_df['IATA'].iloc[0],
                bilhete_df['BILHETE'].iloc[0],
                bilhete_df['CIA'].iloc[0],
                bilhete_df['ORIGEM'].iloc[0],
                destino_final,
                bilhete_df['DATA_EMISSAO'].iloc[0],
                bilhete_df['TARIFA'].iloc[0] * bilhete_df['CAMBIO'].iloc[0],
                pnr,
                bilhete_df['DATA_EMBARQUE'].iloc[0],
                '2 CUPONS (REPROCESSADO)'
            ])
        else:
            # Verificar quantidade de paradas e aplicar a lógica correspondente
            if len(bilhete_df) > 4 and total_paradas_pnr > 2:
                base_tarifaria_anterior = None
                stopover_detectado = False
                destino_final = None  # Inicializar variável do destino final
                 
                # Iterar sobre os cupons para verificar mudanças de base tarifária e stopovers
                for i in range(1, len(bilhete_df)):
                    # Obter datas e calcular diferenças
                    data_embarque_anterior = pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[i - 1], dayfirst=True)
                    data_embarque_atual = pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[i], dayfirst=True)
                    diferenca_dias = (data_embarque_atual - data_embarque_anterior).days

                    hora_pouso_anterior = bilhete_df['HORA_POUSO'].iloc[i - 1]
                    hora_embarque_atual = bilhete_df['HORA_EMBARQUE'].iloc[i]
                    diferenca_horas = abs((hora_embarque_atual - hora_pouso_anterior).total_seconds()) / 3600 if pd.notnull(hora_pouso_anterior) and pd.notnull(hora_embarque_atual) else 0

                    # Identificar se há stopover
                    if diferenca_horas > 14 and diferenca_dias > 1:
                        stopover_detectado = True
                    elif diferenca_horas <= 14 and diferenca_dias <= 1:
                        stopover_detectado = False

                    # Conferir mudanças na base tarifária
                    for i in range(1, len(bilhete_df)):
                        base_tarifaria_atual = bilhete_df['BASE_TARIFARIA'].iloc[i]

                        if base_tarifaria_anterior and base_tarifaria_anterior != base_tarifaria_atual:
                            if diferenca_horas <= 0:
                                destino_final = bilhete_df['DESTINO'].iloc[i - 1]
                                break
                            elif len(bilhete_df) == 4 and origem == ultimo_destino_cupom and abs((bilhete_df['DATA_EMBARQUE'].iloc[1] - bilhete_df['DATA_EMBARQUE'].iloc[0]).days) > 1:
                                destino_final = bilhete_df['DESTINO'].iloc[0]
                                break
                            else:
                                destino_final = bilhete_df['DESTINO'].iloc[i - 1]

                            break
                        elif base_tarifaria_anterior and base_tarifaria_anterior == base_tarifaria_atual:
                            base_tarifaria_unica = bilhete_df['BASE_TARIFARIA'].nunique() == 1

                            if base_tarifaria_unica:
                                for i in range(1, len(bilhete_df)):
                                    data_embarque_anterior = pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[i - 1], dayfirst=True)
                                    data_embarque_atual = pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[i], dayfirst=True)

                                    diferenca_dias = (data_embarque_atual - data_embarque_anterior).days

                                    if diferenca_dias > 1:
                                        destino_final = bilhete_df['DESTINO'].iloc[i - 1]
                                        break
                                    elif len(bilhete_df) == 4 and origem == ultimo_destino_cupom and abs((bilhete_df['DATA_EMBARQUE'].iloc[1] - bilhete_df['DATA_EMBARQUE'].iloc[0]).days) > 1:
                                        destino_final = bilhete_df['DESTINO'].iloc[0]
                                        break

                            elif total_paradas_pnr == 2 and diferenca_dias > 1:
                                destino_final = bilhete_df['DESTINO'].iloc[0]

                            elif diferenca_horas <= 9 and diferenca_dias > 1:
                                destino_final = bilhete_df['DESTINO'].iloc[0]

                            elif base_tarifaria_unica and diferenca_dias <= 1 and diferenca_horas <= 3:
                                destino_final = bilhete_df['DESTINO'].iloc[1]
                                break

                            elif diferenca_dias == 1 and diferenca_horas <= 14:
                                destino_final = bilhete_df['DESTINO'].iloc[1]
                                break
                            

                            else:
                                paradas_identificadas = 0
                                destino_final = None

                                for j in range(1, len(bilhete_df)):
                                    data_embarque_anterior = pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[j - 1], dayfirst=True)
                                    data_embarque_atual = pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[j], dayfirst=True)
                                    hora_embarque_atual = bilhete_df['HORA_EMBARQUE'].iloc[j]
                                    hora_pouso_anterior = bilhete_df['HORA_POUSO'].iloc[j - 1]

                                    diferenca_dias = (data_embarque_atual - data_embarque_anterior).days
                                    diferenca_horas = abs((hora_embarque_atual - hora_pouso_anterior).total_seconds()) / 3600 if pd.notnull(hora_pouso_anterior) and pd.notnull(hora_embarque_atual) else 0

                                    if diferenca_horas > 8 or diferenca_dias > 1:
                                        paradas_identificadas += 1
                                        if paradas_identificadas == 1:
                                            destino_final = bilhete_df['DESTINO'].iloc[j - 1]
                            

                        else:
                            destino_final = bilhete_df['DESTINO'].iloc[i - 1]

                        base_tarifaria_anterior = base_tarifaria_atual

                # FORÇANDO o destino a sempre ser a última parada (como solicitado explicitamente)
                #destino_final = bilhete_df['DESTINO'].iloc[1]

                # Adicionar o registro ao reavaliados
                reavaliados.append([
                    bilhete_df['BASE'].iloc[0],
                    bilhete_df['GOV'].iloc[0],
                    bilhete_df['IATA'].iloc[0],
                    bilhete_df['BILHETE'].iloc[0],
                    bilhete_df['CIA'].iloc[0],
                    bilhete_df['ORIGEM'].iloc[0],
                    destino_final,
                    bilhete_df['DATA_EMISSAO'].iloc[0],
                    bilhete_df['TARIFA'].iloc[0] * bilhete_df['CAMBIO'].iloc[0],
                    pnr,
                    bilhete_df['DATA_EMBARQUE'].iloc[0],
                    'STOPOVER (REPROCESSADO)'
                ])

                #print(f"Bilhete reprocessado para stopover: {bilhete_df['BILHETE'].iloc[0]}, Destino Final: {destino_final}")


            else:
                # Caso de bilhete com menos de 2 paradas (identificação simples)
                origem = bilhete_df['ORIGEM'].iloc[0]
                destino_final = origem
                origem = bilhete_df['ORIGEM'].iloc[0]

                for i in range(1, len(bilhete_df)):
                    data_embarque_anterior = pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[i - 1], dayfirst=True)
                    data_embarque_atual = pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[i], dayfirst=True)
                    diferenca_dias = (data_embarque_atual - data_embarque_anterior).days

                    hora_pouso_anterior = bilhete_df['HORA_POUSO'].iloc[i - 1]
                    hora_embarque_atual = bilhete_df['HORA_EMBARQUE'].iloc[i]

                    diferenca_horas = (hora_embarque_atual - hora_pouso_anterior).total_seconds() / 3600
                    # Se o bilhete tem exatamente 3 cupons e os dois primeiros destinos são iguais
                    if bilhete_df['DESTINO'].iloc[i-1] == bilhete_df['DESTINO'].iloc[i]:
                        destino_final = bilhete_df['ORIGEM'].iloc[i]
                        #print('cond 3', destino_final)
                        break
                        # Se diferença de dias > 1, considerar destino final e sair do loop
                    elif diferenca_horas <= 0 and bilhete_df['DESTINO'].iloc[0] != bilhete_df['DESTINO'].iloc[1]:
                        diferenca_negativa_detectada = True
                        # ➤ Dentro dessa condição, se encontrar diferença de dias > 1, esse é o destino final
                        if diferenca_dias > 1:
                            destino_final = bilhete_df['DESTINO'].iloc[i - 1]

                            break
                        
                    
                    if diferenca_dias > 1 or diferenca_horas <= 8:
                        destino_final = bilhete_df['DESTINO'].iloc[i - 1]                        
                                 
                    if len(bilhete_df) == 4:
                        ultimo_destino_cupom = bilhete_df['DESTINO'].iloc[3]
                        for i in range(len(bilhete_df)):
                            if origem == ultimo_destino_cupom:
                                # Condição prioritária: todos os embarques com espaçamento maior que 1 dia
                                if (
                                    abs((bilhete_df['DATA_EMBARQUE'].iloc[1] - bilhete_df['DATA_EMBARQUE'].iloc[0]).days) > 1 and
                                    abs((bilhete_df['DATA_EMBARQUE'].iloc[2] - bilhete_df['DATA_EMBARQUE'].iloc[1]).days) > 1 and
                                    abs((bilhete_df['DATA_EMBARQUE'].iloc[3] - bilhete_df['DATA_EMBARQUE'].iloc[2]).days) >= 1
                                ):
                                    destino_final = bilhete_df['DESTINO'].iloc[1]
                                else:
                                    destino_final = bilhete_df['DESTINO'].iloc[0]
                                break

                            # elif origem == ultimo_destino_cupom and abs((bilhete_df['DATA_EMBARQUE'].iloc[2] - bilhete_df['DATA_EMBARQUE'].iloc[1]).days) == 1:
                            #     destino_final = bilhete_df['DESTINO'].iloc[1]

                            elif origem != ultimo_destino_cupom and diferenca_dias <= 1:
                                destino_final = bilhete_df['DESTINO'].iloc[3]

                            elif (
                                abs((bilhete_df['DATA_EMBARQUE'].iloc[1] - bilhete_df['DATA_EMBARQUE'].iloc[0]).days) == 0 and
                                abs((bilhete_df['DATA_EMBARQUE'].iloc[2] - bilhete_df['DATA_EMBARQUE'].iloc[1]).days) == 1
                            ):
                                destino_final = bilhete_df['DESTINO'].iloc[2]

                            elif (
                                abs((bilhete_df['DATA_EMBARQUE'].iloc[1] - bilhete_df['DATA_EMBARQUE'].iloc[0]).days) == 0 and
                                abs((bilhete_df['DATA_EMBARQUE'].iloc[2] - bilhete_df['DATA_EMBARQUE'].iloc[1]).days) == 0
                            ):
                                destino_final = bilhete_df['DESTINO'].iloc[2]

                            elif (
                                abs((bilhete_df['DATA_EMBARQUE'].iloc[1] - bilhete_df['DATA_EMBARQUE'].iloc[0]).days) == 1 and
                                abs((bilhete_df['DATA_EMBARQUE'].iloc[2] - bilhete_df['DATA_EMBARQUE'].iloc[1]).days) == 0
                            ):
                                destino_final = bilhete_df['DESTINO'].iloc[2]

                            elif diferenca_dias == 0:
                                destino_final = bilhete_df['DESTINO'].iloc[1]

                            elif diferenca_dias == 1:
                                destino_final = bilhete_df['DESTINO'].iloc[1]


                        break   
                    


                             
                    # Se diferença de dias = 1 e diferença de horas < 11, encontrar último cupom válido
                    if diferenca_dias == 1 and diferenca_horas < 13  and len(bilhete_df) != 4:
                        ultimo_cupom_valido = None
                        destino_final = None  # Inicializando variável
                        
                        for j in range(1, len(bilhete_df)):
                            
                            data_embarque_anterior = pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[j - 1], dayfirst=True)
                            data_embarque_atual = pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[j], dayfirst=True)
                            hora_embarque_atual = bilhete_df['HORA_EMBARQUE'].iloc[j]
                            hora_pouso_anterior = bilhete_df['HORA_POUSO'].iloc[j - 1]

                            diferenca_dias = (data_embarque_atual - data_embarque_anterior).days
                            diferenca_horas = abs((hora_embarque_atual - hora_pouso_anterior).total_seconds()) / 3600 if pd.notnull(hora_embarque_atual) and pd.notnull(hora_pouso_anterior) else 0

                            if diferenca_dias > 1:
                                destino_final = bilhete_df['DESTINO'].iloc[j - 1]  # Última parada antes da diferença > 1 dia
                                break  # Sai do loop assim que encontrar um intervalo maior que 1 dia

                            if diferenca_dias <= 1 and diferenca_horas < 11:
                                ultimo_cupom_valido = j

                        # Se o destino ainda não foi definido pela diferença > 1 dia, usa o último cupom válido
                        if destino_final is None:
                            if ultimo_cupom_valido is not None:
                                destino_final = bilhete_df['DESTINO'].iloc[ultimo_cupom_valido]
                            else:
                                destino_final = bilhete_df['DESTINO'].iloc[0]  # Fallback para o primeiro destino

                        break  # Sai do loop principal

                    # Se há 3 cupons com mesma data de embarque, definir destino final
                    elif len(bilhete_df) == 3 and all(pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[j], dayfirst=True) ==
                                                    pd.to_datetime(bilhete_df['DATA_EMBARQUE'].iloc[0], dayfirst=True) for j in range(1, 3)):
                        origem_primeiro = bilhete_df['ORIGEM'].iloc[0]
                        destino_ultimo = bilhete_df['DESTINO'].iloc[-1]

                        if origem_primeiro != destino_ultimo:
                            destino_final = destino_ultimo
                        else:
                            destino_final = bilhete_df['DESTINO'].iloc[1]

                        break  

                    # Outras condições para determinar destino final
                        
                    if diferenca_dias >= 1 or diferenca_horas >= 12:
                        destino_final = bilhete_df['DESTINO'].iloc[i - 1]
                        break  

                    elif len(bilhete_df) > 2 and diferenca_horas <= 0 and diferenca_dias > 2:
                        destino_final = bilhete_df['DESTINO'].iloc[i - 1]
                        break  

                    elif diferenca_horas <= 0 and len(bilhete_df) > 2:
                        destinos = bilhete_df['DESTINO'].tolist()
                        origens = bilhete_df['ORIGEM'].tolist()

                        frequencias = pd.Series(destinos + origens).value_counts()
                        destino_menos_frequente = frequencias.idxmin()

                        if destino_menos_frequente in origens:
                            frequencias_destinos = pd.Series(destinos).value_counts()
                            destino_menos_frequente = frequencias_destinos.idxmin()

                        if destino_menos_frequente in origens and destino_menos_frequente in destinos:
                            destino_final = destinos[0]
                        else:
                            destino_final = destino_menos_frequente
                        
                        break  

                    if pd.isna(diferenca_dias) or bilhete_df['DESTINO'].isnull().any():
                        destino_final = "NÃO ENCONTRADO"
                        break  

                reavaliados.append([
                    bilhete_df['BASE'].iloc[0],
                    bilhete_df['GOV'].iloc[0],
                    bilhete_df['IATA'].iloc[0],
                    bilhete_df['BILHETE'].iloc[0],
                    bilhete_df['CIA'].iloc[0],
                    origem,
                    destino_final,
                    bilhete_df['DATA_EMISSAO'].iloc[0],
                    bilhete_df['TARIFA'].iloc[0] * bilhete_df['CAMBIO'].iloc[0],
                    pnr,
                    bilhete_df['DATA_EMBARQUE'].iloc[0],
                    'IDENTIFICAÇÃO SIMPLES (REPROCESSADO)'
                ])
                #print('resultado simples',reavaliados)
    # Criar DataFrame para registros reavaliados
    reavaliados_df = pd.DataFrame(reavaliados, columns=['BASE', 'GOV', 'IATA', 'BILHETE', 'CIA', 'ORIGEM', 'DESTINO', 'DATA_EMISSAO', 'TARIFA_BRL', 'PNR_AGENCIA', 'DATA_EMBARQUE', 'OBS'])

    print(f"Quantidade de registros reavaliados: {len(reavaliados_df)}")
    # Identificar bilhetes com DESTINO ou DATA_EMBARQUE vazios

    # Ajuste para incluir os bilhetes com dados incompletos no resultado final
    resultado_final = pd.concat([
    reavaliados_df,
    elegiveis_df
    ], ignore_index=True)

    resultado_final = resultado_final.drop_duplicates(subset=['BILHETE', 'PNR_AGENCIA'])

    print(f"Registros no resultado final: {len(resultado_final)}")

    # Exportar resultado final para Excel
    output_path = r'C:\Users\karen.xavier\Documents\Banco od\test1.xlsx'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    resultado_final.to_excel(output_path, index=False)

    print(f"Resultado final exportado para: {output_path}")

finally:
    try:
        connection.close()
        print("Conexão ao banco de dados encerrada.")
    except NameError:
        pass
    
    
"""brasil_cod_iata = ["GRU", "BSB", "GIG", "SSA"]  # Exemplos de códigos IATA do Brasil
destinos_af_kl = ["CDG", "AMS", "ORY"]

# Filtrando os dados para AF e KL
af_kl = resultado_final[(resultado_final['CIA'].isin(['AF', 'KL'])) &
                        (resultado_final['ORIGEM'].isin(brasil_cod_iata)) &
                        (resultado_final['DESTINO'].isin(destinos_af_kl))]

# Somando os valores para AF e KL
af_kl_total = af_kl['TARIFA_BRL'].sum()

# Filtrando os dados para outras companhias (IND)
ind = resultado_final[~((resultado_final['CIA'].isin(['AF', 'KL'])) &
                        (resultado_final['ORIGEM'].isin(brasil_cod_iata)) &
                        (resultado_final['DESTINO'].isin(destinos_af_kl)))]

# Somando os valores para outras companhias (IND)
ind_total = ind['TARIFA_BRL'].sum()

# Calculando o total geral
total_geral = af_kl_total + ind_total

# Calculando o share
share_af_kl = af_kl_total / total_geral if total_geral != 0 else 0
share_ind = ind_total / total_geral if total_geral != 0 else 0

# Criando o DataFrame final para exibição
resultado_share = pd.DataFrame({
    'Categoria': ['AF_KL', 'IND'],
    'Total': [af_kl_total, ind_total],
    'Share': [share_af_kl, share_ind]
})

# Exibindo o resultado final
print(resultado_share)"""
