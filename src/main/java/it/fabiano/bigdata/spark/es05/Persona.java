package it.fabiano.bigdata.spark.es05;
/**
 * Java-Spark-Training-Course
 *
 * @author  Gaetano Fabiano
 * @version 1.1.0
 * @since   2019-07-19 
 * @updated 2020-07-01 
 */
public class Persona {
	private String nome;
	private String cognome;
	private int eta;
	public Persona() {
		// TODO Auto-generated constructor stub
	}
	public Persona(String nome, String cognome, int eta) {
		super();
		this.nome = nome;
		this.cognome = cognome;
		this.eta = eta;
	}
	public String getNome() {
		return nome;
	}
	public void setNome(String nome) {
		this.nome = nome;
	}
	public String getCognome() {
		return cognome;
	}
	public void setCognome(String cognome) {
		this.cognome = cognome;
	}
	public int getEta() {
		return eta;
	}
	public void setEta(int eta) {
		this.eta = eta;
	}
}
