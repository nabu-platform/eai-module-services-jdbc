/*
* Copyright (C) 2016 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

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
