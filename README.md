# nhl_dw
Se tem um projeto que alavancou habilidades técnicas foi esse.
Talvez os dados de esportes sejam os mais completos, dinâmicos, relativamente complexos e quentes que se possa encontrar, e nessa ocasião apresento os da NHL.
Através de uma API com dezenas de endpoints fiz todo um trabalho de garimpar quais endpoints seriam interessantes, quais se tornaram redudantes, a documentação ajudou e muito até certo ponto
depois disso exigiu muito pensamento analítico, principalmente para a modelagem.
Minha experiência como analista foi crucial pois do começo ao fim eu já tinha em mente como e quais dados eu gostaria de visualizar para analisar.
No entanto, no universo da engenharia é sempre preciso lidar com situações inesperadas e pensar à frente na otimização. Alguns insights e desafios que enfrentei:
 - A importância da modularização de código mas também com a organização;
 - Extração e processamento de dados volumosos é demorado senão aplicar técnicas de paralelismo;
 - Testes unitários pelo menos para checar o output;
 - A primeira carga de dados é sempre mais volumosa e requer utilizar os métodos corretos como COPY ao invés de INSERT;
 - Como garantir consistência nos dados evitar duplicação
 - Modelagem requer uma atenção especial e o dbt é essencial para manter uma documentação mínima e funcional para que os modelos sejam significativos
 - Após a primeira carga como estabilizar e concatenar dados incompletos e atualizados
 - Como reprocessar e recarregar um processo sem onerar a memória e gastar tempo com arquivos já processados. Tabelas que foram carregadas em dias diferentes em algum momento teriam de estar sincronizadas
