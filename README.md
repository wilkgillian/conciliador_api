<h1>Api conciliador</h1>
<br/>
<h2>Api desenvolvida para realizar a conciliação de relatórios financeiros do SENAC-MT.</h2>
<br/>

<h3>Tecnologias:</h3>
<ul>
<li>Python</li>
<li>Pandas</li>
<li>Openpyxl</li>
<li>FastApi</li>
<li>PostgreSQL</li>
<li>Heroku</li>
</ul>

<a href="https://conciliadorapi.herokuapp.com/docs">
Swagger</a>

<span>Obs: a api possui 5 rotas, as 4 primeiras (get, post, put, delete) funcionarão perfeitamente, porém a ultima de get só funcionara se na rota de post forem inseridos os documentos no formato exigido e com os parâmetros exigidos, caso isso não seja cumprido o retorno será um 'Internal server Error'</span>
