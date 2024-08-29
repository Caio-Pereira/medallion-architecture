# Medallion Architecture

## Sobre o projeto
Repositório dedicado para os artefatos necessários para a construção da arquitetura medallion na AWS, com intuito de extrair, transformar e armazenar as informações provenientes da API OpenBreweryDB.

## Arquitetura
A arquitetura Medallion é uma abordagem de design para a organização e processamento de dados em camadas, estruturada em três níveis principais: Bronze, Prata e Ouro. Cada camada tem um propósito específico e contribui para a criação de uma solução de dados escalável e eficiente.

#### Camadas
<b>Camada Bronze:</b> A camada Bronze é responsável pela ingestão e armazenamento dos dados brutos provenientes da API OpenBreweryDB. Nesta camada, os dados são coletados diretamente da fonte, sem transformação significativa, para garantir que todas as informações originais sejam preservadas.

<b>Camada Prata:</b> A camada Prata foca na transformação e limpeza dos dados. Aqui, os dados brutos da camada Bronze são processados para corrigir erros e realizar integrações necessárias. Essa etapa resulta em dados mais estruturados e preparados para análise, a partir da gravação das informações em arquivos do formato Parquet.

<b>Camada Ouro:</b> A camada Ouro é onde os dados são otimizados e preparados para análises avançadas e relatórios. Os dados nesta camada são refinados e agregados para suportar visualizações, dashboards e consultas analíticas complexas, oferecendo insights valiosos para tomada de decisões.
</br> Para esse projeto, duas visualizações são geradas com base nos dados da camada Prata: uma visualização agrupada pelos tipos de cervejarias; e uma visualização da quantidade de cervejarias por cada localização disponível.

#### Ingestão
A ingestão dos dados na camada Bronze é feita por meio do AWS Lambda, que executa requisita as informações presentes na API OpenBreweryDB, e armazena-as no Amazon S3, que atua como o datalake na arquitetura deste projeto.

#### Processamento
O processamento dos dados brutos, e posteriormente das informações presentes na camada Prata, é realizado pelo AWS Glue, um serviço de integração de dados com tecnologia sem servidor que permite a execução em demanda, sem alocação prévia de infraestrutura, de processos baseados no Framework PySpark.

#### Qualidade da Informação
Para garantir que as informações presentes nas visualizações da camada Ouro estejam consistentes, há a aplicação de um processo de checagem dos dados que apenas habilita a atualização dos relatórios quando os dados armazenados na camada Prata atendem à validações de qualidade.

#### Manutenção do Ambiente
Com intuito de manter a performance do fluxo de execução, a arquitetura deste projeto possui um processo, estruturado no AWS Lambda, que movimenta os dados brutos da camada Bronze que foram processados pelo último workflow. Desta forma, o tempo de execução de processamentos futuros não será impactado pelo número crescente de dados brutos no local de leitura.
