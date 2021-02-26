package nabu.services.jdbc.types;

public class JoinStatement {
	// if the join statement can introduce multiple matches, we probably want to add a distinct statement
	// otherwise we might return the original query multiple times
	private Boolean multipleMatches;
	// the source join is the table we are joining on, it must appear in the select statement somewhere
	// and it should be a core table (like node or an extension), not like masterdata being used to resolve to a name or something
	private String sourceJoin;
	// the target join is what you define, this could be as simple as a table or as complex as a subselect
	// it is added verbatim to the sql
	// note that you can _not_ choose the alias that is used, it will be generated depending on how the other aliases are generated
	private String targetJoin;
	// you can decide the "on" query, to reference source and target tables you can use the fixed values source and target
	// these will likely be rewritten to match the aliases that are used at runtime
	// e.g. source.id = target.node_id
	// this might become n.id = na.node_id
	private String on;
	// the default here is "join". but you can also choose "left outer join" for example
	// this is again inserted verbatim
	private String type;
	public Boolean getMultipleMatches() {
		return multipleMatches;
	}
	public void setMultipleMatches(Boolean multipleMatches) {
		this.multipleMatches = multipleMatches;
	}
	public String getSourceJoin() {
		return sourceJoin;
	}
	public void setSourceJoin(String sourceJoin) {
		this.sourceJoin = sourceJoin;
	}
	public String getTargetJoin() {
		return targetJoin;
	}
	public void setTargetJoin(String targetJoin) {
		this.targetJoin = targetJoin;
	}
	public String getOn() {
		return on;
	}
	public void setOn(String on) {
		this.on = on;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
}
