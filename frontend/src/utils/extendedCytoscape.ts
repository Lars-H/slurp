// Cytoscape extensions are initialized here

import cytoscape from "cytoscape";
import cxtmenu from "cytoscape-cxtmenu";
import dagre from "cytoscape-dagre";
import dblclick from "cytoscape-dblclick";
import popper from "cytoscape-popper";
import automove from "cytoscape-automove";

cytoscape.use(dblclick);
cytoscape.use(cxtmenu);
cytoscape.use(dagre);
cytoscape.use(popper);
cytoscape.use(automove);

export default cytoscape;
