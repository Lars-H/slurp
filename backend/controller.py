from nlde.query.sparql_parser import parse
from crop.query_plan_optimizer.ldff_optimizer import LDFF_Optimizer
from crop.query_plan_optimizer.nlde_optimizer import nLDE_Optimizer
from crop.query_plan_optimizer.idp_optimizer import IDP_Optimizer
from crop.costmodel.crop_cost_model import CropCostModel
import logging

logging.getLogger("nlde_logger").setLevel(logging.WARNING)

backend_logger = logging.getLogger("backend")
backend_logger.setLevel(logging.INFO)


def plan_from_optimizer(query_str, sources, optimizer_dct={}):


    backend_logger.info("Optimizer Config: {}".format(optimizer_dct))
    optimizer_name = optimizer_dct.get("name", "left-deep")

    eddies = optimizer_dct.get("eddies", 2)

    pbj_enabled = optimizer_dct.get("pbj", False)
    decomposer_enabled = optimizer_dct.get("decomposer", False)
    pruning_enabled = optimizer_dct.get("pruning", False)

    if optimizer_name == "left-deep" or "left-linear":
        optimizer = LDFF_Optimizer(sources=sources, eddies=eddies, pbj=pbj_enabled, decomposer=decomposer_enabled, pruning=pruning_enabled)

    elif optimizer_name == "nLDE":
        optimizer = nLDE_Optimizer(sources=sources, eddies=eddies)

    elif optimizer_name == "CROP":
        # Cost Model, Robust Model
        cost_model = CropCostModel()
        robust_model = CropCostModel()

        ## IDP Optimizer Setup
        k = optimizer_dct.get("k", 4)
        top_t = optimizer_dct.get("top_t", 5)
        adaptive_k = optimizer_dct.get("adaptive_k", True)

        enable_robustplan = True
        robustness_threshold = optimizer_dct.get("robust_threshold", 0.05)
        cost_threshold = optimizer_dct.get("cost_threshold", 0.3)
        optimizer = IDP_Optimizer(eddies=eddies, sources=sources, cost_model=cost_model,
                                  robust_model=robust_model, k=k, top_t=top_t, adaptive_k=adaptive_k,
                                  enable_robustplan=enable_robustplan,
                                  robustness_threshold=robustness_threshold, cost_threshold=cost_threshold)

    query_parsed = parse(query_str)
    plan = optimizer.create_plan(query_parsed)
    return plan


